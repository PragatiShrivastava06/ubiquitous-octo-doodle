from cement.core.foundation import CementApp
from cement.core.controller import CementBaseController, expose
from pyspark.sql import SparkSession
from pyspark.sql import Row
import matplotlib.pyplot as plt
import unicodedata

# define an application base controller
class MyAppBaseController(CementBaseController):
    class Meta:
        label = 'base'
        description = "My Application parse access logs!"
        epilog = "This is the text at the bottom of --help."

        config_defaults = dict(
            foo='bar',
            some_other_option='my default value',
            )

        arguments = []

    @expose(hide=True, aliases=['run'])
    def default(self):
        self.app.log.info('Not enough sub commands/arguments. Run with "--help" ')

    @expose(aliases=['ps'], help="Plots a histogram of all HTTP statuses.")
    def plot_HTTP_status(self):
        self.app.log.info("Plotting a histogram of all HTTP statuses.")

        logDataset = self.command_Part_4()

        httpStatus = logDataset.selectExpr("cast(status as int) status")
        httpStatus.createOrReplaceTempView("statusFrame")
        df = spark.sql("SELECT * FROM statusFrame WHERE status is NOT NULL")
        m_list = df.selectExpr("status as status")
        status_arr = [int(i.status) for i in m_list.collect()]

        plt.hist(status_arr, bins=100)
        plt.xlabel('HTTP status')
        plt.ylabel('Frequency')
        plt.title(r'Histogram of HTTP Status')
        plt.grid(True)
        plt.show()

        spark.stop()

    @expose(aliases=['rt'], help="Prints the 10 largest request times.")
    def request_time(self):
        self.app.log.info("Printing the 10 largest request times.")

        logDataset = self.command_Part_4()

        logDataset.selectExpr("cast(time as float) time"). \
            orderBy("time", ascending=False). \
            withColumnRenamed("time", "10 Largest Request Time"). \
            show(10)

        spark.stop()

    @expose(aliases=['mm'], help="Prints the mean and median request times.")
    def retrieve_mean_median(self):
        self.app.log.info("Printing the mean and median request times.")

        logDataset = self.command_Part_4()

        timeSorted = logDataset.selectExpr("cast(time as float) time").orderBy("time", ascending=True)
        timeSorted.agg({"time": "avg"}).withColumnRenamed("avg(time)", "Mean Request Time").show()

        cnt = timeSorted.select("time").count()
        rdd = timeSorted.rdd.zipWithIndex()
        flipped = rdd.map(lambda (x, y): (y, x))

        if (cnt % 2 == 0):
            left = cnt / 2 - 1
            right = left + 1
            x = (flipped.lookup(left) + flipped.lookup(right)) / 2
            for x1 in x:
                print ("Media Request Time = %.3f" % x1[0])
        else:
            y = flipped.lookup(cnt / 2)
            for y1 in y:
                print ("Median Request Time = %.3f" % y1[0])

        '''timeSorted.groupBy('time').\
                 agg(F.size(F.collect_list('time'))).\
                 withColumnRenamed("size(collect_list(time))","timeCount").show()'''
        spark.stop()

    @expose(hide=True, aliases=['cmd4'], help="this command gets called by everyone but will not be exposed to user")
    def command_Part_4(self):
        # Create a SparkSession (Note, the config section is only for Windows)
        global spark
        spark = SparkSession.builder.appName("LogHistogram").getOrCreate()

        def mapper(line):
            fields = line.split()
            return Row(time=unicodedata.normalize('NFKD', fields[6]).encode('ascii', 'ignore'), status=fields[10])

        lines = spark.sparkContext.textFile("test-access.log")
        logs = lines.map(mapper)

        # Convert that to a DataFrame
        return spark.createDataFrame(logs).cache()

class MyApp(CementApp):
    class Meta:
        label = 'example'
        base_controller = MyAppBaseController

with MyApp() as app:
    app.run()