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
             [independent :as independent]
             [nemesis :as nemesis]
             [os :as os]
             [store :as store]
             [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.util :refer [meh]]
            [jepsen.control.net :as net]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [register-service.client :as rs]
            [register-service.handler :refer [resource-url]]
            [failjure.core :as f]))

(def register-service-url-default
  "https://github.com/ivankelly/register-service/releases/download/v0.2.0/register-service-0.2.0-standalone.jar")
(def bookkeeper-server-tarball-url-default
  "http://apache.rediris.es/bookkeeper/bookkeeper-4.6.0/bookkeeper-server-4.6.0-bin.tar.gz")

(def register-service-port 3111)
(def register-service-dir "/opt/register-service")
(def register-service-log-file "/var/log/register-service/register-service.log")
(def register-service-systemd-name "register-service")

(def bookkeeper-systemd-name "bookkeeper")
(def bookkeeper-dir "/opt/bookkeeper")
(def bookkeeper-log-file "/var/log/bookkeeper/bookkeeper.log")

(def zookeeper-data-dir "/var/lib/zookeeper")
(def zookeeper-log-file "/var/log/zookeeper/zookeeper.log")

(def debian-stretch
  (reify os/OS
    (setup! [_ test node]
       (meh (net/heal)))
    (teardown! [_ test node])))

(defn db
  "Install bookkeeper and zookeeper on nodes"
  [version bookkeeper-tarball register-service-uberjar]
  (reify
    db/DB
    (setup! [a test node]
      (loop [seconds 30]
        (if (= seconds 0)
          (Exception. "Register service not up")
          (let [url (resource-url node register-service-port "key1")
                result (rs/get-value url)]
            (if (f/failed? result)
              (do
                (Thread/sleep 1000)
                (recur (- seconds 1)))
              (info node "all up"))))))
    (teardown! [b test node])
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
  (reify client/Client
    (setup! [_ test node]
      (client node))

    (invoke! [this test op]
      (let [[k v] (:value op)
            url (resource-url server register-service-port (str "key" k))]
        (println "key " k)
        (case (:f op)
          :read (let [res (rs/get-value url)]
                  (if (f/failed? res)
                    (do
                      (assoc op :type :fail))
                    (let [seq-no (:seq res)]
                      (assoc op :type :ok
                             :value (independent/tuple k (:value res))))))
          :write (let [res (rs/set-value! url v)]
                   (assoc op :type (ok-fail-or-unknown? res)))
          :cas   (let [[value value'] v
                       get-res (rs/get-value url)]
                   (if (or (f/failed? get-res)
                           (not= (:value get-res) value))
                     (assoc op :type :fail)
                     (let [seq-no (:seq get-res)
                           set-res (rs/set-value! url value' :seq-no seq-no)]
                       (assoc op :type (ok-fail-or-unknown? set-res))))))))

    (teardown! [_ test])))

(defn bk-test
  [opts]
  (merge tests/noop-test
         {:name "bk"
          :os debian-stretch
          :db (db "4.4.0"
                  (:bookkeeper-tarball opts)
                  (:register-service-uberjar opts))
          :client (client nil)
          :nemesis (nemesis/partition-random-halves)
          :model  (model/cas-register 0)
          :checker (checker/compose
                    {:perf (checker/perf)
                     :timeline (timeline/html)
                     :linear (independent/checker checker/linearizable)})
          :generator (->> (independent/concurrent-generator
                           5
                           (range)
                           (fn [k]
                             (->> (gen/mix [r w cas])
                                  (gen/stagger (/ 1 (:requests-per-second opts)))
                                  (gen/limit 100))))
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
    :validate [pos? "Must be positive"]]
   [nil "--bookkeeper-tarball TARBALL" "BookKeeper server tarball"
    :default bookkeeper-server-tarball-url-default]
   [nil "--register-service-uberjar JAR"
    "Register service uberjar to use"
    :default register-service-url-default]])

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
