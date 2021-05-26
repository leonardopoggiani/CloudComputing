import pandas as pd
import matplotlib.pyplot as plt

dataframe = pd.read_csv("C:\\Users\Leonardo Poggiani\\Documents\\GitHub\\covid19-opendata-vaccini\\dati\\vaccini-summary-latest.csv")

plt.plot(dataframe['area'], dataframe['dosi_somministrate'], 'r--')
plt.plot(dataframe['area'], dataframe['dosi_consegnate'])
plt.xticks(rotation=35)
plt.grid()

plt.show()

dataframe['differenza_consegnate_somministrate'] = dataframe['dosi_consegnate'] - dataframe['dosi_somministrate']
print(dataframe[['area', 'dosi_somministrate', 'differenza_consegnate_somministrate']])

plt.plot(dataframe['area'], dataframe['differenza_consegnate_somministrate'])
plt.xticks(rotation=35)
plt.grid()

plt.show()

dataframe = pd.read_csv("C:\\Users\Leonardo Poggiani\\Documents\\GitHub\\covid19-opendata-vaccini\\dati\\somministrazioni-vaccini-latest.csv")
