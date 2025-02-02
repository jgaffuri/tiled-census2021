from pygridmap import gridtiler
from datetime import datetime
import os


aggregated_folder = "/home/juju/geodata/census/2021/aggregated/"


transform = True
aggregate = False
tiling = False

# fid,GRD_ID,T,M,F,Y_LT15,Y_1564,Y_GE65,EMP,NAT,EU_OTH,OTH,SAME,CHG_IN,CHG_OUT,LAND_SURFACE,POPULATED,COUNT,
# T_CI,M_CI,F_CI,Y_LT15_CI,Y_1564_CI,Y_GE65_CI,EMP_CI,NAT_CI,EU_OTH_CI,OTH_CI,SAME_CI,CHG_IN_CI,CHG_OUT_CI

#GRD_ID,T,M,F,Y_LT15,Y_1564,Y_GE65,EMP,NAT,EU_OTH,OTH,SAME,CHG_IN,CHG_OUT,LAND_SURFACE,POPULATED,CONFIDENTIALSTATUS
#transform

if transform:
    def tr(c):

        # skip non populated cells
        if c['T'] == "0": return False

        # remove useless information
        del c['fid']
        #del c['LAND_SURFACE']
        del c['POPULATED']
        del c['COUNT']

        #extrac x,y
        gid = c['GRD_ID'].replace("CRS3035RES1000mN", "").split('E')
        del c['GRD_ID']
        c['x'] = gid[1]
        c['y'] = gid[0]

        # fix: mark data as not available
        # not available data is marked as ""
        if( c['M'] == "0" and c['F'] == "0" ):
            c['M'] = ""
            c['F'] = ""
        if( c['Y_LT15'] == "0" and c['Y_1564'] == "0" and c['Y_GE65'] == "0" ):
            c['Y_LT15'] = ""
            c['Y_1564'] = ""
            c['Y_GE65'] = ""
        if( c['NAT'] == "0" and c['EU_OTH'] == "0" and c['OTH'] == "0" ):
            c['NAT'] = ""
            c['EU_OTH'] = ""
            c['OTH'] = ""
        if( c['SAME'] == "0" and c['CHG_IN'] == "0" and c['CHG_OUT'] == "0" ):
            c['SAME'] = ""
            c['CHG_IN'] = ""
            c['CHG_OUT'] = ""

        # ensures confidentialstatus is 0 or 1
        #if c['CONFIDENTIALSTATUS'] == "": c['CONFIDENTIALSTATUS'] = 0
        for cc in [ "T_CI", "M_CI", "F_CI", "Y_LT15_CI", "Y_1564_CI", "Y_GE65_CI", "EMP_CI", "NAT_CI", "EU_OTH_CI", "OTH_CI", "SAME_CI", "CHG_IN_CI", "CHG_OUT_CI" ]:
            if c[cc] == "": c[cc] = 0
            elif c[cc] == "0": c[cc] = 0
            elif c[cc] == "-9999": c[cc] = 1
            else: print(c[cc])



    gridtiler.grid_transformation("/home/juju/geodata/census/2021/ESTAT_Census_2021_V2.csv", tr, aggregated_folder+"1000.csv")



#aggregation
if aggregate:

    # the aggregation function:
    # as soon as one value is NA, then the aggregated is NA
    def aggregation_sum_NA(values, _=0):
        sum = 0
        for value in values:
            if value == "": return ""
            sum += float(value)
        return sum
    aggregation_fun = {}
    for code in ["M","F","Y_LT15","Y_1564","Y_GE65","EMP","NAT","EU_OTH","OTH","SAME","CHG_IN","CHG_OUT"]: aggregation_fun[code] = aggregation_sum_NA

    # launch aggregations
    for a in [2,5,10]:
        print(datetime.now(), "aggregation to", a*1000, "m")
        gridtiler.grid_aggregation(input_file=aggregated_folder+"1000.csv", resolution=1000, output_file=aggregated_folder+str(a*1000)+".csv", a=a, aggregation_fun=aggregation_fun)
    for a in [2,5,10]:
        print(datetime.now(), "aggregation to", a*10000, "m")
        gridtiler.grid_aggregation(input_file=aggregated_folder+"10000.csv", resolution=10000, output_file=aggregated_folder+str(a*10000)+".csv", a=a, aggregation_fun=aggregation_fun)




# tiling
if tiling:
    for resolution in [1000, 2000, 5000, 10000, 20000, 50000, 100000]:
        print(datetime.now(), "tiling for resolution", resolution)

        #create output folder
        out_folder = 'pub/v2/parquet/' + str(resolution)
        if not os.path.exists(out_folder): os.makedirs(out_folder)

        # launch tiling
        gridtiler.grid_tiling(
            aggregated_folder+str(resolution)+".csv",
            out_folder,
            resolution,
            tile_size_cell = 256,
            x_origin = 0,
            y_origin = 0,
            format = "parquet"
        )
