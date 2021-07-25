import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val stationList = spark.read.option("header", true).csv("/FileStore/tables/stationlist.csv")
val countryList = spark.read.option("header", true).csv("/FileStore/tables/countrylist.csv")
val globalWeather = spark.read.option("header", true).csv("/FileStore/tables/data")

val fullCountryDf = stationList.join(countryList, "COUNTRY_ABBR")
fullCountryDf.show

//use a broadcast join on the smaller dataframe
val globalWeatherDf = globalWeather.join(broadcast(fullCountryDf), globalWeather("STN---") === fullCountryDf("STN_NO"))
globalWeatherDf.show

//country with highest mean temp
val hottestDf = globalWeatherDf.groupBy("COUNTRY_FULL").agg(mean("TEMP")).orderBy(desc("avg(TEMP)")).limit(1)
hottestDf.show

// country with most consecutive days of tornadoes/funnel 
// look at FRSHTT column, sixth digit must be '1'


// First cut down the dataframe by filtering for only days with Tornados
val tornadoDf = globalWeatherDf.filter(globalWeatherDf("FRSHTT").rlike("1$") )

//convert YEARMODA to proper date column
val tornadoDates = tornadoDf.withColumn("date", to_timestamp(col("YEARMODA"),"yyyyMMdd"))
val tornadoDatesB = tornadoDates.withColumnRenamed("COUNTRY_FULL","COUNTRY")
//Perform a self-join on tornadoDates, if there is a match between a country, and a day vs day-1, then return the row
val tornadoDatesNew = tornadoDates.join(tornadoDatesB, tornadoDates("COUNTRY_ABBR") === tornadoDatesB("COUNTRY_ABBR") && tornadoDates("DATE") === date_sub(tornadoDates("DATE"),1), "left" )

//Aggregate/groupBy count the data
val tornadoConsecutive = tornadoDatesNew.groupBy("COUNTRY_FULL").count.orderBy(asc("count"))
tornadoConsecutive.show

//country with second highest mean wind speed
//wording of question is mildly ambiguous.  If we're looking for the second highest wdsp for each country, we would use a window function.  Otherwise, if we're looking for the country with the secound highest wdsp, then just do groupby/agg(mean)/orderByDesc, and the last row of the subset of the first two rows

val windiestDf = globalWeatherDf.groupBy("COUNTRY_FULL").agg(mean("WDSP")).orderBy(desc("avg(WDSP)"))
windiestDf.take(2).last

//val windowSpec  = Window.partitionBy("COUNTRY_FULL").orderBy(desc("WDSP"))
//globalWeatherDf.withColumn("row_number",row_number.over(windowSpec)).show(100,false)
