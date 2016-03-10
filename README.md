## cassandra-storage-options

Written to demonstrate the performance difference between writing ordinary
cassandra tables and writing columnar blobs. The impact for rage reads is
tremendous, you loose the ability to query specific data, it will only work if
your data is write only (no updates).

###to run

Pull the data from http://download.cms.gov/openpayments/PGYR14_P011516.ZIP

extract it


We will use the General payments file, remove the header:

and run the program:

sbt "run ./filename.csv"

You will be prompted to pick a main class, run them sequentially to perform the
case class (regular cassandra table) test and the data table test (columnar
storage).
