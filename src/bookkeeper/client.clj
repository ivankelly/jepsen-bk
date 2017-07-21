(ns bookkeeper.client
  "Bookkeeper client for clojure"
  (:import [org.apache.bookkeeper.client LedgerHandle LedgerEntry BookKeeper
            BookKeeper$DigestType AsyncCallback$AddCallback BKException
            BKException$Code AsyncCallback$AddCallback]
           [org.apache.bookkeeper.conf ClientConfiguration]))

(def default-bookkeeper-opts {:zookeeper/connect "localhost"})

(def default-ledger-opts {:ensemble-size 3
                          :quorum-size 2
                          :digest-type BookKeeper$DigestType/MAC
                          :password (.getBytes "bk-jepsen")})

(defn ^org.apache.bookkeeper.client.BookKeeper bookkeeper
  [opts]
  (let [opts (merge default-bookkeeper-opts opts)]
    (let [conf (doto (ClientConfiguration.)
                 (.setZkServers (:zookeeper/connect opts)))]
      (BookKeeper. conf))))

;; (defn open-ledger ^org.apache.bookkeeper.client.LedgerHandle
;;   [^BookKeeper client ledger-id]
;;   (open-ledger client ledger-id {})
;;   [^BookKeeper client id opts]
;;   (let [opts (merge *default-ledger-opts* opts)]
;;     (.openLedger client
;;                  ledger-id
;;                  (:digest-type opts)
;;                  (:password opts))))

;; (defn create-ledger ^org.apache.bookkeeper.client.LedgerHandle
;;   [^BookKeeper client]
;;   (create-ledger client {})
;;   [^BookKeeper client opts]
;;   (let [opts (merge *default-ledger-opts* opts)]
;;     (.createLedger client
;;                    (:ensemble-size opts)
;;                    (:quorum-size opts)
;;                    (:digest-type opts)
;;                    (:password opts))))

;; (defn close-handle [^LedgerHandle ledger-handle]
;;   (.close ledger-handle))

;; (defn foo
;;   [arg1 rest]
;;   (println (:foobar rest)))

