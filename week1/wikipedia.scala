// Initialization 
case class WikipediaArticle(title: String, text: String)
val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

val wikiArticles = sc.textFile("dbfs:/FileStore/tables/wikipedia.dat", 1)
val subs = "</title><text>"
val rdd = wikiArticles.map(line => WikipediaArticle(line.substring(14, line.indexOf(subs)),line.substring(line.indexOf(subs) + subs.length, line.length-16)))

// Part 1: Naive ranking
val start = System.currentTimeMillis()


val rankLang = langs.map(lang => (lang,
                                  rdd.aggregate(0)((count, article) => article.text.split(' ').contains(lang) match {case true => count + 1 case _ => count + 0},(a, b) => a + b)
                                 )).sortBy(_._2).reverse

val stop = System.currentTimeMillis()
println(s"Processing Part 1: naive ranking took ${stop - start} ms.\n")

// Part2: Ranking using inverted index
val index = rdd.flatMap(a => langs.map(l => a.text.split(' ').contains(l) match {case true => (l,a) case false => null} )).filter(_ != null).groupByKey()

val start2 = System.currentTimeMillis()

val rankByIndex = index.mapValues(articles => articles.count(_ => true)).collect().toList.filter(_._2 != null).sortBy(_._2).reverse

val stop2 = System.currentTimeMillis()
println(s"Processing Part 2: ranking using inverted index took ${stop2 - start2} ms.\n")

// Part 3: ranking using reduceByKey
val start3 = System.currentTimeMillis()

val rankReduceByKey = rdd.flatMap(a => langs.map(l => (l, a.text.split(' ').contains(l) match {case true => 1 case false => 0} ))).reduceByKey((x,y) => x + y).collect().toList.filter(_ != null).sortBy(_._2).reverse

val stop3 = System.currentTimeMillis()
println(s"Processing Part 3: ranking using reduceByKey took ${stop3 - start3} ms.\n")