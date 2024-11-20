import pandas as pd
from pygridmap import gridtiler
from datetime import datetime
import os


aggregated_folder = "/home/juju/geodata/census/2021/aggregated/"


transform = True
aggregate = True
tiling = True


#GRD_ID,T,M,F,Y_LT15,Y_1564,Y_GE65,EMP,NAT,EU_OTH,OTH,SAME,CHG_IN,CHG_OUT,LAND_SURFACE,POPULATED,CONFIDENTIALSTATUS
#transform

if transform:
    def tr(c):
        #print(c)
        if c['T'] == "0": return False
        del c['LAND_SURFACE']
        del c['POPULATED']
        gid = c['GRD_ID'].replace("CRS3035RES1000mN", "").split('E')
        del c['GRD_ID']
        c['x'] = gid[1]
        c['y'] = gid[0]

        if c['EMP'] == "": c['EMP'] = "NA"

        if c['CONFIDENTIALSTATUS'] == "": c['CONFIDENTIALSTATUS'] = 0

        #for k, v in c.items():
        #    if v == "" or v == None: print(k)

    gridtiler.grid_transformation("/home/juju/geodata/census/2021/ESTAT_Census_2021_V2.csv", tr, aggregated_folder+"1000.csv")



#aggregation
if aggregate:

    def aggregation_sum_EMP(values, _=0):
        sum = 0
        for value in values:
            if value == "NA": return "NA"
            sum += float(value)
        return sum
    aggregation_fun = { 'EMP':aggregation_sum_EMP }

    for a in [2,5,10]:
        print(datetime.now(), "aggregation to", a*1000, "m")
        gridtiler.grid_aggregation(input_file=aggregated_folder+"1000.csv", resolution=1000, output_file=aggregated_folder+str(a*1000)+".csv", a=a, aggregation_fun=aggregation_fun)
    for a in [2,5,10]:
        print(datetime.now(), "aggregation to", a*10000, "m")
        gridtiler.grid_aggregation(input_file=aggregated_folder+"10000.csv", resolution=10000, output_file=aggregated_folder+str(a*10000)+".csv", a=a, aggregation_fun=aggregation_fun)




#tiling
if tiling:
    for resolution in [1000, 2000, 5000, 10000, 20000, 50000, 100000]:
        print("tiling for resolution", resolution)

        #create output folder
        out_folder = 'pub/v2/parquet/' + str(resolution)
        if not os.path.exists(out_folder): os.makedirs(out_folder)

        gridtiler.grid_tiling(
            aggregated_folder+str(resolution)+".csv",
            out_folder,
            resolution,
            tile_size_cell = 256,
            x_origin = 0,
            y_origin = 0,
            format = "parquet"
        )
