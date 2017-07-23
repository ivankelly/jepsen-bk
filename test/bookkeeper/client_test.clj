(ns bookkeeper.client-test
  (:require [clojure.test :refer :all]
            [bookkeeper.client :as bkc]
            [bookkeeper.mini-cluster :as mc]))

(def ^:dynamic *zkconnect* nil)

(defn bk-fixture
  [f]
  (let [cluster (mc/create 3)]
    (mc/start cluster)
    (binding [*zkconnect* (mc/zookeeper-connect-string cluster)]
      (f))
    (mc/kill cluster)))

(use-fixtures :each bk-fixture)

(defn entries-eq?
  "Compare two sequences of entries,
   fixing up java arrays so they can be compared"
  [a b]
  (let [fixup (fn [e] [(first e) (seq (second e))])]
    (= (map fixup a)
       (map fixup b))))

(deftest bk-client-test
  (testing "BookKeeper client operations"
    (let [bk (bkc/bookkeeper {:zookeeper/connect *zkconnect*})
          ledger @(bkc/create-ledger bk)]
      (try
        (is (= @(bkc/add-entry ledger (byte-array '(1))) 0))
        (is (= @(bkc/add-entry ledger (byte-array '(2))) 1))
        (is (= @(bkc/add-entry ledger (byte-array '(3))) 2))

        (let [ledger-id (bkc/ledger-id ledger)
              read-ledger @(bkc/open-ledger-no-recovery bk ledger-id)
              lac (bkc/last-add-confirmed read-ledger)]
          (is (= lac 1))
          (is (entries-eq? @(bkc/read-entries read-ledger 0 lac)
                           (list [0 (seq '(1))] [1 (seq '(2))]))))

        @(bkc/close-ledger ledger)

        (let [ledger-id (bkc/ledger-id ledger)
              read-ledger @(bkc/open-ledger bk ledger-id)
              lac (bkc/last-add-confirmed read-ledger)]
          (is (= lac 2))
          (is (entries-eq? @(bkc/read-entries read-ledger 0 lac)
                           (list [0 (seq '(1))]
                                 [1 (seq '(2))]
                                 [2 (seq '(3))]))))

        (finally
          @(bkc/close-ledger ledger)
          (bkc/close bk))))))


