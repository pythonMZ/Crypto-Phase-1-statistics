from zipfile import ZipFile
import pandas as pd


zip = ZipFile("csv_365.zip") 
name=zip.namelist()
zip.extractall()


l=pd.read_csv(name[0])
col=list(l.columns)[0].split(';')
col.insert(0,'Name')


o=[]
for i in name:
    h=pd.read_csv(i)
    n=i.split('_')[0]
    for j in list(h.index):
        gg=h.iloc[j,0].split(';')
        gg.insert(0,n)
        o.append(gg)


new_df=pd.DataFrame(o,columns=col)
new_df.to_excel('final_scrap.xlsx',index=False)
