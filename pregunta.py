"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import re 


def ingest_data():

    #
    # Inserte su código aquí
    #
    
    columNames=[(0, 7), (7, 23), (23, 39), (39, 119)]
    namecol=pd.read_fwf("clusters_report.txt",header=None,nrows=2, colspecs=columNames,skip_blank_lines=False)
    Noncolum=[]
    nombreCol=""
    for j in range(0,len(namecol.columns)):
        if type(namecol.iloc[1][j])==str:
            nombreCol=namecol.iloc[0][j] +" "+ namecol.iloc[1][j]
        else:
            nombreCol=namecol.iloc[0][j]
        Noncolum.append(nombreCol.lower().replace(" ","_"))                   
        
            

    columnas=[(0, 5), (5, 12), (12, 31), (31, 119)]
    Predf=pd.read_fwf("clusters_report.txt",header=None,skiprows=4, colspecs=columnas,skip_blank_lines=False, names=Noncolum)

    cluster=Predf["cluster"]
    cluster=cluster.dropna(ignore_index=True)
    cantPalClav=Predf["cantidad_de_palabras_clave"]
    cantPalClav=cantPalClav.dropna(ignore_index=True)
    porcPalClav=Predf["porcentaje_de_palabras_clave"].astype("string")
    porcPalClav=porcPalClav.dropna(ignore_index=True)
    porcPalClav=porcPalClav.str.rstrip("%").str.replace(",",".").str.replace(" ","").astype(float)
    

    palCla=pd.Series(name=Predf["principales_palabras_clave"].name)

    pal=""
    j=0
    for i in range(0, len(Predf)):
        if type(Predf.iloc[i]["principales_palabras_clave"])==str:
            pal=pal+Predf.iloc[i]["principales_palabras_clave"]+" "
        else:
            pal=pal.replace(".", "").replace("\n","").strip()
            palCla[j]=re.sub("\s{2,}"," ",pal)
            pal=""
            j=j+1

    df=pd.concat([cluster, cantPalClav, porcPalClav, palCla],axis=1,ignore_index=False)
    return df

DF=ingest_data()