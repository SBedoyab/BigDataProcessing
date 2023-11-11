// Databricks notebook source
// MAGIC %md
// MAGIC ---
// MAGIC ###TODOS LOS PUNTOS ESTÁN ORDENADOS EN SOLO UN NOTEBOOK
// MAGIC ___

// COMMAND ----------

// MAGIC %md
// MAGIC ---
// MAGIC 1. ¿Cuál es el país más “feliz” del 2021 según la data?

// COMMAND ----------

// Importaciones necesarias
import org.apache.spark.sql.functions.{col, desc, row_number}

  //Para crear una ventana
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// Lectura de dataframe
val df_1 = spark.read
  .option("header", true)
  .option("inferSchema","true")
  .csv("/FileStore/datasets/world_happiness_report_2021.csv")

df_1.printSchema

// COMMAND ----------

display(df_1)

// COMMAND ----------

// Creación de la ventana
val dfTemp = Window.orderBy(desc("Ladder score"))

// COMMAND ----------

val dfRanking = df_1.withColumn("Ranking", row_number().over(dfTemp)).select(
  col("Country name"),
  col("Ladder score"),
  col("Ranking")
)

display(dfRanking)

// COMMAND ----------

// MAGIC %md
// MAGIC Se concluye que "Finland" es el país más feliz. 
// MAGIC Esto se afirma debido a haberlo ordenarlo por la columna "Ladder score" de manera descendente y con ayuda de la librería Window hacer un ranking para así definir las posiciones.
// MAGIC ---

// COMMAND ----------

// MAGIC %md
// MAGIC ---
// MAGIC 2. ¿Cuál es el país más “feliz” del 2021 por continente según la data?

// COMMAND ----------

// Importaciones necesarias
import org.apache.spark.sql.functions.{col, desc, row_number, when}

  //Para crear una ventana
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// Lectura de dataframe
val df_2 = spark.read
  .option("header", true)
  .option("inferSchema","true")
  .csv("/FileStore/datasets/world_happiness_report_2021.csv")

// COMMAND ----------

 // .distinct para saber regiones únicas
df_2.select(col("Regional indicator")).distinct.show(false)

// COMMAND ----------

// Catalogar las regiones en continente 
val dfContinent = df_2.withColumn("continent",
  when(col("Regional indicator") === "South Asia", "Asia")
  .when(col("Regional indicator") === "Middle East and North Africa", "Asia")
  .when(col("Regional indicator") === "North America and ANZ" && (col("Country name") === "New Zealand" || col("Country name") === "Australia"), "Oceania")
  .when(col("Regional indicator") === "North America and ANZ", "America")
  .when(col("Regional indicator") === "Sub-Saharan Africa", "Africa")
  .when(col("Regional indicator") === "East Asia", "Asia")
  .when(col("Regional indicator") === "Commonwealth of Independent States", "Europe")
  .when(col("Regional indicator") === "Latin America and Caribbean", "America")
  .when(col("Regional indicator") === "Western Europe", "Europe")
  .when(col("Regional indicator") === "Central and Eastern Europe", "Europe")
  .when(col("Regional indicator") === "Southeast Asia", "Asia")
)

display(dfContinent.select(col("Country name"), col("Regional indicator"), col("continent")).orderBy("continent"))

/*
  Hay que tener en cuenta que, debido a que la mayoría de países en "Commonwealth of Independent States" son pertenecientes oficialmente a Europa, interpreté que podía decidir si ponerlos en Asia o Europa y decidí hacerlo en Europa por lo anterior explicado, caso similar a la región "Middle East and North Africa", que por no alterar el resultado del ranking, decidí poner esta región en Asia y no en África
*/

// COMMAND ----------

// Separación de los continentes en sus respectivos dataframes
val dfAsia = dfContinent.filter(col("continent") === "Asia")

val dfEurope = dfContinent.filter(col("continent") === "Europe")

val dfAmerica = dfContinent.filter(col("continent") === "America")

val dfAfrica = dfContinent.filter(col("continent") === "Africa")

val dfOceania = dfContinent.filter(col("continent") === "Oceania")

// COMMAND ----------

// Creación de la ventana como en ejercicio 1
val dfTemp = Window.orderBy(desc("Ladder score"))

// COMMAND ----------

val dfRankingAsia = dfAsia.withColumn("Ranking", row_number().over(dfTemp)).select(
  col("Country name"),
  col("Ladder score"),
  col("Ranking")
)

display(dfRankingAsia)

// COMMAND ----------

val dfRankingEurope = dfEurope.withColumn("Ranking", row_number().over(dfTemp)).select(
  col("Country name"),
  col("Ladder score"),
  col("Ranking")
)

display(dfRankingEurope)

// COMMAND ----------

val dfRankingAmerica = dfAmerica.withColumn("Ranking", row_number().over(dfTemp)).select(
  col("Country name"),
  col("Ladder score"),
  col("Ranking")
)

display(dfRankingAmerica)

// COMMAND ----------

val dfRankingAfrica = dfAfrica.withColumn("Ranking", row_number().over(dfTemp)).select(
  col("Country name"),
  col("Ladder score"),
  col("Ranking")
)

display(dfRankingAfrica)

// COMMAND ----------

val dfRankingOceania = dfOceania.withColumn("Ranking", row_number().over(dfTemp)).select(
  col("Country name"),
  col("Ladder score"),
  col("Ranking")
)

display(dfRankingOceania)

// COMMAND ----------

// MAGIC %md
// MAGIC - País más feliz de Asia en el 2021: Israel
// MAGIC - País más feliz de Europa en el 2021: Finlandia
// MAGIC - País más feliz de America en el 2021: Canadá
// MAGIC - País más feliz de Africa en el 2021: Mauricio (Mauritius)
// MAGIC - País más feliz de Oceania en el 2021: Nueva Zelanda
// MAGIC ---

// COMMAND ----------

// MAGIC %md
// MAGIC ---
// MAGIC 3. ¿Cuál es el país que más veces ocupó el primer lugar en todos los años?

// COMMAND ----------

// Importaciones necesarias
import org.apache.spark.sql.functions.{col, desc, when, lit,rank}

  //Para crear una ventana
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// Lectura de dataframes
val df_3_2021 = spark.read
  .option("header", true)
  .option("inferSchema", "true")
  .csv("/FileStore/datasets/world_happiness_report_2021.csv")

val df_3_ = spark.read
  .option("header", true)
  .option("inferSchema", "true")
  .csv("/FileStore/datasets/world_happiness_report.csv")
df_3_.printSchema

// COMMAND ----------

// Preparación de los dataframes para hacer la agregación

val df_3_2021agg = df_3_2021
  .withColumn("year", lit(2021))
  .withColumnRenamed("Ladder score", "Life Ladder")
  .withColumnRenamed("Healthy life expectancy", "Healthy life expectancy at birth")
  .withColumnRenamed("Logged GDP per capita", "Log GDP per capita")

val clean_df_3_2021 = df_3_2021agg
  .select(
    col("Country name") 
  , col("year") 
  , col("Life Ladder") 
  , col("Log GDP per capita") 
  , col("Social support") 
  , col("Healthy life expectancy at birth") 
  , col("Freedom to make life choices") 
  , col("Generosity") 
  , col("Perceptions of corruption")
  )

  val clean_df_3_ = df_3_
  .select(
    col("Country name") 
  , col("year") 
  , col("Life Ladder") 
  , col("Log GDP per capita") 
  , col("Social support") 
  , col("Healthy life expectancy at birth") 
  , col("Freedom to make life choices") 
  , col("Generosity") 
  , col("Perceptions of corruption")
  )

// COMMAND ----------

// Union

val df_all_years = clean_df_3_.union(clean_df_3_2021)

display(df_all_years)

// COMMAND ----------

display(df_all_years.orderBy(desc("year"),desc("Life Ladder")))

// COMMAND ----------

// Ventana para particionar por año ordenado por el "score" de felicidad
val dfTemp_3 = Window.partitionBy("year").orderBy(desc("Life Ladder"))

// COMMAND ----------

// Selección por primer lugar
val df_First_Place = df_all_years.withColumn("ranking", rank().over(dfTemp_3))
                                 .filter(col("ranking") === 1)
                                 .select("Country name", "year")
display(df_First_Place)

// COMMAND ----------

// Agrupación y conteo
val df_First_Place_Sum = df_First_Place.groupBy("Country name").count()

display(df_First_Place_Sum)

// COMMAND ----------

// MAGIC %md
// MAGIC - Debido a una igual cantidad de veces que los paises de Dinamarca y Finlandia obtuvieron el título de "los más felices", hay un empate entre los mencionados. Sin embargo, debido a que Finlandia ha estado obteniendo el título desde el año 2016 en adelante, hay una gran probabilidad que en el siguiente año también siga siendo el país más feliz
// MAGIC ---

// COMMAND ----------

// MAGIC %md
// MAGIC ---
// MAGIC 4. ¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?

// COMMAND ----------

// Importaciones necesarias
import org.apache.spark.sql.functions.{col, desc, row_number}
 
 //Para crear una ventana
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// Carga de datos
val df_4 = spark.read
                .option("header", true)
                .option("inferSchema", "true")
                .csv("/FileStore/datasets/world_happiness_report.csv")

df_4.printSchema()

// COMMAND ----------

// Creación de ventana

val dfTemp_4 = Window.orderBy(desc("Life Ladder"))

// COMMAND ----------

val df_4_filter = df_4.filter(col("year") === 2020 && col("Log GDP per capita").isNotNull)

val df_4_Ranking = df_4_filter.withColumn("ranking", row_number().over(dfTemp_4))
                              .select(
                                col("Country name")
                              , col("ranking")
                              , col("Life Ladder")
                              , col("Log GDP per capita")
                              , col("year")
                              )
                              .orderBy("Log GDP per capita")

display(df_4_Ranking)

// COMMAND ----------

// MAGIC %md
// MAGIC - El país con mayor GDP en el año 2020 fue Uganda, con un puesto de ranking 77 en el ladder global. A tener en consideración que se omitieron valores nulos en la columna "Log GDP per capita" para liempieza en el resultado
// MAGIC ---
