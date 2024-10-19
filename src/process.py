import pandas as pd
from pygridmap import gridtiler
from datetime import datetime
import os

transform = True


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
        if c['CONFIDENTIALSTATUS'] == "": c['CONFIDENTIALSTATUS'] = 0
        return c
    gridtiler.grid_transformation("/home/juju/geodata/census/2021/ESTAT_Census_2021_V2.csv", tr, "/home/juju/geodata/census/tmp/1000.csv")







exit()

format_join = False
aggregate = False
tiling = False

folder = "/home/juju/geodata/elections_fr/leg2024/"


#aggregation
if aggregate:

    #codeDepartement	nomDepartement	codeCirconscription	nomCirconscription	codeCommune	nomCommune	codeBureauVote
    aggregation_fun = {
        "codeDepartement": gridtiler.aggregation_single_value,
        "nomDepartement": gridtiler.aggregation_single_value,
        "codeCirconscription": gridtiler.aggregation_single_value,
        "nomCirconscription": gridtiler.aggregation_single_value,
        "codeCommune": gridtiler.aggregation_single_value,
        "nomCommune": gridtiler.aggregation_single_value,
        #TODO "codeBureauVote": gridtiler.aggregation_single_value,
        }

    print(datetime.now(), "aggregation to", 100, "m")
    gridtiler.grid_aggregation(input_file=folder + "1.csv", resolution=1, output_file=folder+"100.csv", a=100, aggregation_fun=aggregation_fun)

    for a in [2,5,10]:
        print(datetime.now(), "aggregation to", a*100, "m")
        gridtiler.grid_aggregation(input_file=folder+"100.csv", resolution=100, output_file=folder+str(a*100)+".csv", a=a, aggregation_fun=aggregation_fun)
    for a in [2,5,10]:
        print(datetime.now(), "aggregation to", a*1000, "m")
        gridtiler.grid_aggregation(input_file=folder+"1000.csv", resolution=1000, output_file=folder+str(a*1000)+".csv", a=a, aggregation_fun=aggregation_fun)
    for a in [2,5,10]:
        print(datetime.now(), "aggregation to", a*10000, "m")
        gridtiler.grid_aggregation(input_file=folder+"10000.csv", resolution=1000, output_file=folder+str(a*10000)+".csv", a=a, aggregation_fun=aggregation_fun)

    print(datetime.now(), "clean")
    for resolution in [100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000]:
        f = folder+str(resolution)+".csv"
        df = pd.read_csv(f)
        df.loc[df['nb_bv'] > 1, ['codeDepartement', 'nomDepartement', 'codeCirconscription', 'codeCommune', 'nomCirconscription', 'nomCommune', 'codeBureauVote']] = None
        df.to_csv(f, index=False)



#tiling
if tiling:
    for resolution in [100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000]:
        print("tiling for resolution", resolution)

        #create output folder
        out_folder = 'pub/gridviz/leg2024/T1_bv/' + str(resolution)
        if not os.path.exists(folder): os.makedirs(folder)

        gridtiler.grid_tiling(
            folder+str(resolution)+".csv",
            out_folder,
            resolution,
            tile_size_cell = 256,
            x_origin = 0,
            y_origin = 0,
            #crs = "EPSG:3035",
            format = "parquet"
        )