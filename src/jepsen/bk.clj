(ns jepsen.bk
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen
             [checker :as checker]
             [cli :as cli]
             [client :as client]
             [control :as c]
             [core :as jepsen]
             [db :as db]
             [generator :as gen]
             [nemesis :as nemesis]
             [os :as os]
             [store :as store]
             [tests :as tests]]
            [jepsen.util :refer [meh]]
            [jepsen.control.net :as net]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [register-service.client :as rs]
            [register-service.handler :refer [resource-url]]
            [failjure.core :as f]))

(def register-service-url
  "https://github.com/ivankelly/register-service/releases/download/v0.2.0/register-service-0.2.0-standalone.jar")
(def register-service-port 3111)
(def register-service-dir "/opt/register-service")
(def register-service-log-file "/var/log/register-service.log")
(def register-service-systemd-name "register-service")

(def bookkeeper-systemd-name "bookkeeper")
(def bookkeeper-dir "/opt/bookkeeper")
(def bookkeeper-log-file "/var/log/bookkeeper.log")

(def zookeeper-data-dir "/var/lib/zookeeper")
(def zookeeper-log-file "/var/log/zookeeper/zookeeper.log")

(def debian-stretch
  (reify os/OS
    (setup! [_ test node]
      (info node "setting up debian")
      (binding [c/*sudo* nil]
        (c/exec :apt-get :install :-y :--force-yes :sudo))
      (debian/setup-hostfile!)
      (debian/maybe-update!)
      (debian/install [:wget
                       :curl
                       :vim
                       :man-db
                       :faketime
                       :ntpdate
                       :unzip
                       :iptables
                       :psmisc
                       :tar
                       :bzip2
                       :openjdk-8-jdk
                       :iputils-ping
                       :iproute
                       :rsyslog
                       :logrotate])
      (meh (net/heal)))
    (teardown! [_ test node])))

(defn- zoo-cfg-ids
  "Get a deterministic mapping of zookeeper nodes to ids"
  [zk-nodes]
  (zipmap (sort zk-nodes) (map #(+ 1 %) (range))))

(defn- zoo-cfg-servers
  "Build list of servers for zoo.cfg"
  [zk-nodes]
  (->>
   (map (fn [pair]
          (let [host (first pair)
                id (second pair)]
            (str "server." id "=" host ":2888:3888")))
        (zoo-cfg-ids zk-nodes))
   (str/join "\n")))

(defn- zoo-cfg
  "Build zoo.cfg from a list of servers"
  [zk-nodes]
  (str
   (slurp (clojure.java.io/resource "zoo.cfg"))
   "\n"
   (zoo-cfg-servers zk-nodes)))

(defn- install-zk!
  "Install zookeeper on node"
  [nodes node]
  (info node "installing zookeeper")
  (debian/install [:zookeeperd :zookeeper :zookeeper-bin])
  (c/exec :echo (zoo-cfg nodes) :> "/etc/zookeeper/conf/zoo.cfg")
  (c/exec :mkdir :-p zookeeper-data-dir)
  (c/exec :chown :-R "zookeeper:zookeeper" zookeeper-data-dir)
  (c/exec :echo (get (zoo-cfg-ids nodes) node) :>
          (str zookeeper-data-dir "/myid"))
  (c/exec :service :zookeeper :restart))

(defn- teardown-zk!
  "Tear down zookeeper on a node"
  [node]
  (c/exec :service :zookeeper :stop)
  (c/exec :rm :-rf zookeeper-data-dir)
  (c/exec :rm :-f zookeeper-log-file))

(defn- zk-connect-string
  "Build a zookeeper connect string from a set of servers"
  [zk-nodes]
  (str/join "," zk-nodes))

(defn- bookie-server-cfg
  "Build bookie server config file contents"
  [zk-nodes]
  (str
   (slurp (clojure.java.io/resource "bk_server.conf"))
   "\nzkServers=" (zk-connect-string zk-nodes) "\n"))

(defn- create-systemd-unit-file!
  [node name directory & args]
  (let [localfile (str "/tmp/" node "-" name ".service")
        remotefile (str "/etc/systemd/system/" name ".service")]
    (spit
     localfile
     (str "[Unit]
Description=" name "
After=network.target

[Service]
ExecStart=" (str/join " " args) "
WorkingDirectory=" directory "
RestartSec=1s
Restart=on-failure
Type=simple

[Install]
WantedBy=multi-user.target
"))
    (c/upload localfile remotefile)))

(def jepsen-start-file "/tmp/jepsen-start")

(defn- mark-jepsen-start! []
  (c/exec :touch jepsen-start-file))

(defn- since-start []
  (c/exec :stat :-c :%y jepsen-start-file :| :cut :-c :1-19))

(defn- dump-journal [unit file]
  (c/exec :journalctl :-u unit :-S (since-start) :> file))

(defn- install-register-service!
  "Install register service on node"
  [nodes node]
  (info node "installing register service")
  (c/exec :mkdir :-p register-service-dir)

  (binding [c/*dir* register-service-dir]
    (c/exec :curl :-L :-o "register-service.jar"
            register-service-url)
    (create-systemd-unit-file!
     node register-service-systemd-name register-service-dir
     "/usr/bin/java" "-jar" "register-service.jar"
     "-z" (zk-connect-string nodes)
     "-p" (str register-service-port)
     "-t" (str 6000)) ; 6sec is minimum for default zk setup
    (c/exec :systemctl :daemon-reload)
    (c/exec :systemctl :start register-service-systemd-name)))

(defn- teardown-register-service!
  "Tear down register service on a node"
  [node]
  (binding [c/*dir* register-service-dir]
    (c/exec :systemctl :stop register-service-systemd-name)
    (dump-journal register-service-systemd-name
                  register-service-log-file)))

(defn- install-bk!
  "Install bookkeeper on node"
  [version nodes node]
  (info node "installing bookkeeper")
  (cu/install-tarball!
   node
   (str
    "http://apache.rediris.es/bookkeeper/bookkeeper-" version
    "/bookkeeper-server-" version "-bin.tar.gz")
   bookkeeper-dir)
  (binding [c/*dir* bookkeeper-dir]
    (c/exec :echo (bookie-server-cfg nodes) :> "conf/bk_server.conf")
    (c/exec "bin/bookkeeper" :shell :bookieformat
            :--deleteCookie :--force :--nonInteractive)
    (if (= (first (sort nodes)) node)
      (c/exec "bin/bookkeeper" :shell :metaformat
              :--force :--nonInteractive))
    (c/exec :mkdir :-p "logs")
    (create-systemd-unit-file!
     node bookkeeper-systemd-name
     bookkeeper-dir (str bookkeeper-dir "/bin/bookkeeper") "bookie")
    (c/exec :systemctl :daemon-reload)
    (c/exec :systemctl :start bookkeeper-systemd-name)))

(defn- teardown-bk!
  "Tear down bookkeeper on a node"
  [node]
  (info node "tearing down bookkeeper")
  (c/exec :systemctl :stop bookkeeper-systemd-name)
  (try
    (c/exec :pkill :-f :-9 "bookkeeper")
    (catch RuntimeException e)) ; ignore
  (c/exec :rm :-rf (str bookkeeper-dir "/bk-journal"))
  (c/exec :rm :-rf (str bookkeeper-dir "/bk-data"))
  (dump-journal bookkeeper-systemd-name bookkeeper-log-file))

(defn db
  "Install bookkeeper and zookeeper on nodes"
  [version]
  (reify
    db/DB
    (setup! [a test node]
      (info node "installing something" version)
      (mark-jepsen-start!)
      (let [nodes (:nodes test)]
        (install-zk! nodes node)
        (install-bk! version nodes node)
        (install-register-service! nodes node))
      (info node "all installed, waiting for up")
      (loop [seconds 30]
        (if (= seconds 0)
          (Exception. "Register service not up")
          (let [url (resource-url node register-service-port)
                result (rs/get-value url)]
            (if (f/failed? result)
              (do
                (Thread/sleep 1000)
                (recur (- seconds 1)))
              (info node "all up"))))))
    (teardown! [b test node]
      (teardown-register-service! node)
      (teardown-bk! node)
      (teardown-zk! node))
    db/LogFiles
    (log-files [_ test node]
      [register-service-log-file
       bookkeeper-log-file
       zookeeper-log-file])))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn- ok-fail-or-unknown? [res]
  (if (f/failed? res)
    (cond
      (= (f/message res) "Connection refused") :fail
      :else :info)
    (if res :ok :fail)))

(defn client
  "A client to access the register service"
  [server]
  (let [url (resource-url server register-service-port)]
    (reify client/Client
      (setup! [_ test node]
        (client node))

      (invoke! [this test op]
        (case (:f op)
          :read (let [res (rs/get-value url)]
                  (if (f/failed? res)
                    (do
                      (assoc op :type :fail))
                    (let [seq-no (:seq res)]
                      (assoc op :type :ok
                             :value (:value res)))))
          :write (let [value (:value op)
                       res (rs/set-value! url value)]
                   (assoc op :type (ok-fail-or-unknown? res)))
          :cas   (let [[value value'] (:value op)
                       get-res (rs/get-value url)]
                   (if (or (f/failed? get-res)
                           (not= (:value get-res) value))
                     (assoc op :type :fail)
                     (let [seq-no (:seq get-res)
                           set-res (rs/set-value! url value' :seq-no seq-no)]
                       (assoc op :type (ok-fail-or-unknown? set-res)))))))

      (teardown! [_ test]))))

(defn bk-test
  [opts]
  (merge tests/noop-test
         {:name "bk"
          :os debian-stretch
          :db (db "4.4.0")
          :client (client nil)
          :nemesis (nemesis/partition-random-halves)
          :model  (model/cas-register 0)
          :checker checker/linearizable
          :generator (->> (gen/mix [r w cas])
                          (gen/stagger (/ 1 (:requests-per-second opts)))
                          (gen/nemesis
                           (gen/seq (cycle [(gen/sleep 5)
                                            {:type :info, :f :start}
                                            (gen/sleep (+ 5 (rand-int 5)))
                                            {:type :info, :f :stop}])))
                          (gen/time-limit (:time-limit opts)))}
         opts))

(def bk-opts
  "Extra command line options for the bookkeeper test"
  [[nil "--requests-per-second RPS"
    "Number of requests to make per second"
    :default 1
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]])

(defn check-run! [timestamp]
  "Load a run and analyze with the linearizable checker"
  (try
    (jepsen/log-results
     (let [test (store/load "bk" timestamp)
           result (checker/check-safe
                   checker/linearizable
                   test
                   (model/cas-register 0)
                   (:history test))]
       (info result)
       (when (:name result) (store/save-2! result))
       (when-not (:valid? (:results result))
         (System/exit 1))))
    (finally
      (store/stop-logging!))))


(def check-cmd
  {"check" {:opt-spec [cli/help-opt
                       [nil "--run-timestamp TIMESTAMP"
                        "Timestamp of run to check"]]
            :run (fn [{:keys [options]}]
                   (check-run! (:run-timestamp options)))}})

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn bk-test
                                         :opt-spec bk-opts})
                   (cli/serve-cmd)
                   check-cmd)
            args))
