;   Copyright (c) Rich Hickey. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file epl-v10.html at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.

(ns org.clojure.rdfm
  (:import 
   (java.util UUID)
   (javax.xml.datatype DatatypeFactory XMLGregorianCalendar)
   (org.openrdf.repository Repository RepositoryConnection)
   (org.openrdf.model BNode Graph Literal Namespace Resource Statement URI Value ValueFactory)
   (org.openrdf.repository.sail SailRepository)
   (org.openrdf.repository.http HTTPRepository)
   (org.openrdf.sail.memory MemoryStore)))

(set! *warn-on-reflection* true)
(set! *print-meta* false)
(def #^Repository repo (SailRepository. (MemoryStore. (java.io.File. "/Users/rich/dev/data/db"))))
(.initialize repo)
(def #^"[Lorg.openrdf.model.Resource;" NOCONTEXT (make-array org.openrdf.model.Resource 0))
(def KEYWORDS "http://clojure.org/keywords/")
(def #^String RDF "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
(def #^RepositoryConnection conn (.getConnection repo))
(.setNamespace conn "kw" "http://clojure.org/keywords/")
(.getNamespace conn "kw")
(def #^ValueFactory vf (.getValueFactory conn))
(def #^DatatypeFactory dtf (DatatypeFactory/newInstance))
(.close conn)
(.shutDown repo)

;import java.util.GregorianCalendar;
;import javax.xml.datatype.DatatypeFactory;
;import javax.xml.datatype.XMLGregorianCalendar
;GregorianCalendar c = new GregorianCalendar();
;c.setTime(yourDate);
;XMLGregorianCalendar date2 = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);

(defn kw->uri [k]
  (.createURI vf KEYWORDS (subs (str k) 1)))

(defn uri->kw [uri]
  (keyword (subs (str uri) (count KEYWORDS))))

(defn random-uuid-uri []
  (java.net.URI. (str "urn:uuid:" (java.util.UUID/randomUUID))))

(defn random-bnode-id 
  ([] (random-bnode-id ""))
  ([prefix] (str "_:" prefix (java.util.UUID/randomUUID))))

(str (.createBNode vf))
(.getLocalName (.createURI vf  (str "urn:uuid:" (java.util.UUID/randomUUID))))

(.createBNode vf  (str (java.util.UUID/randomUUID))) 
(java.net.URI. (str "urn:uuid:" (java.util.UUID/randomUUID)))

(declare store-1)

(defn- resource-for [id]
  (condp instance? id
    Resource id
    java.net.URI  (.createURI vf (str id))
    UUID (.createURI vf "urn:uuid:" (str id))
    String (if (.startsWith #^String id "_:")
             (.createBNode vf (subs id 2))
             (.createURI vf id))))

(defn- value-for [o]
  (condp instance? o
    clojure.lang.IPersistentCollection (do (assert (::id ^o)) (resource-for (::id ^o)))
    Value o
    String (.createLiteral vf #^String o)
    Integer (.createLiteral vf (int o))
    Long (.createLiteral vf (long o))
    Float (.createLiteral vf (float o))
    Double (.createLiteral vf (double o))
    Boolean (.createLiteral vf (boolean o))
    java.net.URI  (.createURI vf (str o))
    clojure.lang.Keyword (kw->uri o)
    BigInteger (.createLiteral vf (str o) (.createURI vf "http://www.w3.org/2001/XMLSchema#integer"))
    BigDecimal (.createLiteral vf (str o) (.createURI vf  "http://www.w3.org/2001/XMLSchema#decimal"))
    XMLGregorianCalendar (.createLiteral vf #^XMLGregorianCalendar o)))


(def value-extractors
     {"" #(.getLabel #^Literal %)
      "http://www.w3.org/2001/XMLSchema#int" #(.intValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#long" #(.longValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#float" #(.floatValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#double" #(.doubleValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#boolean" #(.booleanValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#integer" #(.integerValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#decimal" #(.decimalValue  #^Literal %)
      
      "http://www.w3.org/2001/XMLSchema#dateTime" #(.calendarValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#time" #(.calendarValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#date" #(.calendarValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#gYearMonth" #(.calendarValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#gMonthDay" #(.calendarValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#gYear" #(.calendarValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#gMonth" #(.calendarValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#gDay" #(.calendarValue  #^Literal %)})

(defn- extract-value [v]
  (if (instance? Resource v)
    (if (.startsWith (str v) KEYWORDS) 
      (uri->kw v) 
      v)
    ((value-extractors (str (.getDatatype #^Literal v))) v)))
  

(defn- property-uri [k]
  (cond 
   (string? k) (.createURI vf k)
   (keyword? k) (kw->uri k)
   :else (throw (IllegalArgumentException. (str "Unsupported key type: " (class k))))))

(defn- property-key [p]
  (let [id (str p)]
    (if (.startsWith id KEYWORDS)
      (uri->kw p)
      id)))

(defn- add-statement [s p o]
  (.add conn (.createStatement vf s p o) NOCONTEXT))

(defn store-meta [o]
  (let [ret-meta (dissoc ^o ::id)]
    (if (pos? (count ret-meta))
      (let [m (store-initial ret-meta (random-bnode-id))]
        (add-statement (resource-for (::id ^o)) (property-uri ::meta) (resource-for (::id ^m)))
        (with-meta o (assoc m ::id (::id ^o))))
      o)))

(defmulti store-initial (fn [o & id] (class o)))

(defmethod store-initial :default [x & id] x)

(defmethod store-initial clojure.lang.IPersistentVector [vec]
  (let [id (random-bnode-id "__vec__")
        res (resource-for id)
        ret (reduce (fn [vret i]
                      (let [v (store-initial (vec i))]
                        (add-statement res (.createURI  vf RDF (str "_" (inc i))) (value-for v))
                        (conj vret v)))
                    [] (range (count vec)))
        ;ret (with-meta ret (assoc (meta vec) ::id id))
        ;ret (store-meta ret)
        ret (with-meta ret {::id id})]
    ret))

(defmethod store-initial clojure.lang.IPersistentMap
  ([m] (store-initial m (random-bnode-id)))
  ([m id]
     (let [res (resource-for id)
           ret (reduce 
                (fn [mret [k v]]
                  (letfn [(store1 [v] 
                            (let [nested (store-initial v)]
                              (add-statement res (property-uri k) (value-for nested))
                              nested))]
                    (cond
                     (set? v) (assoc mret k (reduce (fn [s v] (conj s (store1 v))) #{} v))
                     :else (assoc mret k (store1 v)))))
                {} m)
           ret (with-meta ret (assoc (meta m) ::id id))
           ret (store-meta ret)]
       ret)))

(defn store-root 
  ([m] (store-root m (random-uuid-uri)))
  ([m uri] (store-initial m uri)))
  
(defn get-statements [id]
  (let [res (.getStatements conn (resource-for id) nil nil false NOCONTEXT)
        ret (into [] (.asList res))]
    (.close res)
    ret))

(defn nuke [id]
  (.remove conn (resource-for id) nil nil NOCONTEXT))

(declare reload)

(defn restore-value [value]
  (let [v (extract-value value)]
    (condp instance? v
        BNode (reload v)
        URI (java.net.URI. (str v))
        v)))

(defn setify [x]
  (if (set? x) x #{x}))

(defn uri->id [uri]
  (condp instance? uri
    BNode (str uri)
    String uri
    java.net.URI uri
    URI (java.net.URI. (str uri))))

(defn reload [id]
  (let [statements (get-statements id)
        id-str (str id)]
    (cond
     (.startsWith id-str "_:__vec__") (with-meta
                                       (reduce (fn [v #^Statement s]
                                                 (let [p (.getPredicate s)
                                                       i  (dec (-> p str (subs 44) Integer/parseInt))
                                                       o (.getObject s)]
                                                   (assoc v i (restore-value o))))
                                               (vec (repeat (count statements) nil))
                                               statements)
                                       {::id id-str})
     :else (vary-meta
            (reduce (fn [m #^Statement s]
                      (let [k (property-key (.getPredicate s))
                            v (restore-value (.getObject s))]
                        (cond 
                         (= k ::meta) (with-meta m v)
                         (contains? m k) (assoc m k (conj (setify (m k)) v))
                         :else (assoc m k v)))) {} statements)
            assoc ::id (uri->id id)))))
                         
    
  

