(defproject jepsen.bk "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.bk
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.4"]
                 [org.apache.bookkeeper/bookkeeper-server "4.4.0"
                  :exclusions [[org.slf4j/slf4j-log4j12]]]
                 [org.apache.bookkeeper/bookkeeper-server "4.4.0"
                  :classifier "tests"
                  :exclusions [[org.slf4j/slf4j-log4j12]]]
                 [org.apache.zookeeper/zookeeper "3.4.6"
                  :classifier "tests"
                  :exclusions [[org.slf4j/slf4j-log4j12]]]
                 [junit/junit "4.12"]])
