(ns jepsen.bk
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen
             [cli :as cli]
             [control :as c]
             [db :as db]
             [os :as os]
             [tests :as tests]]
            [jepsen.util :refer [meh]]
            [jepsen.control.net :as net]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(defn zk-nodes
  [nodes]
  (set (take 3 nodes)))

(defn zk-node?
  [nodes node]
  (contains? (zk-nodes nodes) node))

(defn bk-nodes
  [nodes]
  (set (drop (min 3 (- (count nodes) 3)) nodes)))

(defn bk-node?
  [nodes node]
  (contains? (bk-nodes nodes) node))

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
  [zk-nodes node]
  (info node "installing zookeeper")
  (debian/install [:zookeeperd :zookeeper :zookeeper-bin])
  (c/exec :echo (zoo-cfg zk-nodes) :> "/etc/zookeeper/conf/zoo.cfg")
  (c/exec :mkdir :-p "/var/lib/zookeeper")
  (c/exec :chown :-R "zookeeper:zookeeper" "/var/lib/zookeeper")
  (c/exec :echo (get (zoo-cfg-ids zk-nodes) node) :> "/var/lib/zookeeper/myid")
  (c/exec :service :zookeeper :restart))

(defn- teardown-zk!
  "Tear down zookeeper on a node"
  [node]
  (c/exec :service :zookeeper :stop)
  (c/exec :rm :-rf "/var/lib/zookeeper"))

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

(defn- install-bk!
  "Install bookkeeper on node"
  [version bk-nodes zk-nodes node]
  (info node "installing bookkeeper")
  (cu/install-tarball!
   node
   (str
    "http://apache.rediris.es/bookkeeper/bookkeeper-" version
    "/bookkeeper-server-" version "-bin.tar.gz")
   "/opt/bookkeeper")
  (binding [c/*dir* "/opt/bookkeeper"]
    (c/exec :echo (bookie-server-cfg zk-nodes) :> "conf/bk_server.conf")
    (c/exec "bin/bookkeeper" :shell :bookieformat
            :--deleteCookie :--force :--nonInteractive)
    (if (= (first (sort bk-nodes)) node)
      (c/exec "bin/bookkeeper" :shell :metaformat
              :--force :--nonInteractive))
    (c/exec :mkdir :-p "logs")
    (cu/start-daemon! {:logfile "logs/stderr.log"
                       :pidfile "logs/bookie.pid"
                       :chdir "/opt/bookkeeper"}
                      "bin/bookkeeper" "bookie")))

(defn- teardown-bk!
  "Tear down bookkeeper on a node"
  [node]
  (info node "tearing down bookkeeper")
  (binding [c/*dir* "/opt/bookkeeper"]
    (cu/stop-daemon! "logs/bookie.pid"))
  (try
    (c/exec :pkill :-f :-9 "bookie")
    (catch RuntimeException e)) ; ignore
  (c/exec :rm :-rf "/opt/bk-journal")
  (c/exec :rm :-rf "/opt/bk-data"))

(defn db
  "Install bookkeeper and zookeeper on nodes"
  [version]
  (reify db/DB
    (setup! [a test node]
      (info node "installing something" version)
      (let [nodes (:nodes test)
            zk-nodes (zk-nodes nodes)
            bk-nodes (bk-nodes nodes)]
        (if (contains? zk-nodes node)
          (install-zk! zk-nodes node))
        (if (contains? bk-nodes node)
          (install-bk! version bk-nodes zk-nodes node))))
    (teardown! [b test node]
      (let [nodes (:nodes test)
            zk-nodes (zk-nodes nodes)
            bk-nodes (bk-nodes nodes)]
        (if (contains? bk-nodes node)
          (teardown-bk! node))
        (if (contains? zk-nodes node)
          (teardown-zk! node))))))

(defn bk-test
  [opts]
  (merge tests/noop-test
         {:name "bk"
          :os debian-stretch
          :db (db "4.4.0")}
         opts))

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn bk-test})
                   (cli/serve-cmd))
            args))
