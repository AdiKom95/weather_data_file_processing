# -*- coding: utf-8 -*-

"""
Created on Mon Mar  4 13:00:31 2019

@author: Adi
"""



"""
Niniejszy skrypt ma za zadanie w odpowiedni sposób przetworzyc dane meteorologiczne, następnie
przekazac je do bazy danych i dolaczyc do pliku zawierajacego informacje o pozarach.

Głowne składowe skryptu:
    
1. Przygotowanie srodowiska pracy
   (ladowanie bibliotek, ustalanie katalogu roboczego)

2. Selekcja danych 
   (wybor plikow .csv z danymi meteorologicznymi)

3. Załadowanie danych do pamieci i ich wstepne przetwarzanie

4. Przeksztalcenie danych do postaci DataFrame, uzupelnianie brakow danych, przetwarzanie do formy docelowej - umozliwiajacej eksport do bazy danych

5. Obsługa bazy danych
   (Nawiazanie polaczenia z baza danych, eksport danych do SZBD Postgres, dolaczenie danych meteorologicznych do danych o pozarach)


"""


### 1


import os
import pandas as pd
import numpy as np
import psycopg2 
import sqlalchemy as sa


sciezka = "D://meteo//meteo_synop//Poznan" # katalog z danymi meteo
os.chdir(sciezka)
os.getcwd()

 
"""
lista_plikow=[]
for i in range(len(lista_plikow_0)):
    if lista_plikow_0[i].endswith(".csv")==True and len(lista_plikow_0[i])==18:
        lista_plikow.append(lista_plikow_0[i])

"""        
#########################

## W kolejnych czesciach skryptu lietry "A" oraz "B" odnosily sie beda do rozniacych sie parametrami meteorologicznymi plikow

### 2


lista_plikow_0=os.listdir('.') # wszystkie pliki w katalogu roboczym

## A

lista_plikow=[plik for plik in lista_plikow_0 if plik.endswith(".csv") and len(plik)==18]

## B

lista_plikow_2=[plik for plik in lista_plikow_0 if plik.endswith(".csv") and len(plik)==16]


#########################

### 3


## A

# Otwieranie plikow meteo i wczytywanie ich zawartosci do zmiennej

wczytany=[]
for plik in lista_plikow:
    with open(plik) as pliczek:
        wczytany.append(pliczek.read())


# Tworzenie listy list zawierającej poszczególne charakterystyki pomiarów

wczytany_2=[plik.split("\n") for plik in wczytany] # podział po znaku konńca linii

wczytany_3=[] 
for plik in wczytany_2:
    for wiersz in plik:
        wczytany_3.append(wiersz.split(","))



## B
        
wczytany_b =[]
for plik in lista_plikow_2:
    with open(plik) as pliczek:
        wczytany_b.append(pliczek.read())


# Tworzenie listy list zawierającej poszczególne charakterystyki pomiarów

wczytany_2b=[plik.split("\n") for plik in wczytany_b] # podział po znaku konńca linii

wczytany_3b=[] 
for plik in wczytany_2b:
    for wiersz in plik:
        wczytany_3b.append(wiersz.split(","))




##############################

### 4
        
## A

ramka_danych=pd.DataFrame(wczytany_3)

# selecja interesujacych atrybutow

ramka_wybrane=ramka_danych[[0,2,3,4,7,9,13]]
ramka_wybrane.columns=["kod_stacji","rok","miesiac","dzien","v_wiatru","temp","wilg"]



# operacja uzupelniania brakow w danych + konwersja danych na float

df2=ramka_wybrane.copy()

puste=[]

for i in range(4,7):
    for j in range(ramka_wybrane.shape[0]):
        try:
            if ramka_wybrane.iloc[j,i][0] == ".":
                df2.iloc[j,i]=ramka_wybrane.iloc[j,i].replace('.','0.')
                df2.iloc[j,i]=float(df2.iloc[j,i])
            elif ramka_wybrane.iloc[j,i][0] == "-" and ramka_wybrane.iloc[j,i][1] == ".":
                df2.iloc[j,i]=ramka_wybrane.iloc[j,i].replace('-','-0')
                df2.iloc[j,i]=float(df2.iloc[j,i])
            else:
                df2.iloc[j,i]=float(df2.iloc[j,i])
        except:
            print("Napotkano na NULL w wierszu: ",j)
            puste.append(j)
            
# usuwanie wierszy pustych

do_usuniecia=list(np.unique(puste))

df2=df2.drop(do_usuniecia)


# tworzenie kolumny - identyfikatora, pozwalajacej na wykoanie pozniejszego laczenia tabel

df2["data_t"]=df2["dzien"]+"-"+df2["miesiac"]+"-"+df2["rok"]


df2["data_t"]=df2["data_t"].replace(['"'],[''], regex=True)

df2.iloc[:,[0,1,2,3]]=df2.iloc[:,[0,1,2,3]].replace(['"'],[''], regex=True)


## B

ramka_danych_b=pd.DataFrame(wczytany_3b)

ramka_wybrane_b=ramka_danych_b[[0,2,3,4,13,15,20]]

ramka_wybrane_b.columns=["kod_stacji","rok","miesiac","dzien","opad","rdz_opa","uslon"]


df2b=ramka_wybrane_b.copy()

puste_b=[]

for i in [4,6]:
    for j in range(ramka_wybrane_b.shape[0]):
        try:
            if ramka_wybrane_b.iloc[j,i][0] == ".":
                df2b.iloc[j,i]=ramka_wybrane_b.iloc[j,i].replace('.','0.')
                df2b.iloc[j,i]=float(df2b.iloc[j,i])
            elif ramka_wybrane_b.iloc[j,i][0] == "-" and ramka_wybrane_b.iloc[j,i][1] == ".":
                df2b.iloc[j,i]=ramka_wybrane_b.iloc[j,i].replace('-','-0')
                df2b.iloc[j,i]=float(df2b.iloc[j,i])
            else:
                df2b.iloc[j,i]=float(df2b.iloc[j,i])
        except:
            print("Napotkano na NULL w wierszu: ",j)
            puste_b.append(j)

do_usuniecia_b=list(np.unique(puste_b))

df2b=df2b.drop(do_usuniecia_b)

df2b["data_tb"]=df2b["dzien"]+"-"+df2b["miesiac"]+"-"+df2b["rok"]

df2b["data_tb"]=df2b["data_tb"].replace(['"'],[''], regex=True)

df2b.iloc[:,[0,1,2,3]]=df2.iloc[:,[0,1,2,3]].replace(['"'],[''], regex=True)

# edytowanie kolumny 'rodzaj opadu'
df2b["rdz_opa"]=df2b["rdz_opa"].replace(['""""'],['B'], regex=False)
df2b["rdz_opa"]=df2b["rdz_opa"].replace(['""'],['B'], regex=False)
df2b["rdz_opa"]=df2b["rdz_opa"].replace(['"'],[''], regex=True)



##################################

### 5



url = "postgresql+psycopg2://postgres:haslo@localhost:5432/nazwa_bazy_danych"

try:
    silnik = sa.create_engine(url) # tworzenie silnika do bazy danych
    print("Wszystko ok")
except:
    print("Cos poszło nie tak!")

try:
    eng_connect=silnik.connect()
    print("Połączono!")
except:
    print("Ups...")
    
#** zapytanie testowe do relacji pozary

query = "SELECT * FROM pozary WHERE blad in('OK','KO')"

try:
    result = eng_connect.execute(query)
    print("Zapytanie zostało wykonane...")
except:
    print("Ups...")    

rows = []

for i in range(result.rowcount) :
    rows.append(list(result.fetchone()))

#**

# eksport ramek danych do bazy

df2.to_sql('dane_meteo', silnik, schema='public', if_exists='replace') 

df2b.to_sql('dane_meteo_b', silnik, schema='public', if_exists='replace') 



# zapytanie łączące tabele w widok

query2 = """CREATE OR REPLACE  VIEW pozary_meteo as (
SELECT pozary.*, dane_meteo.*, dane_meteo_b.opad, dane_meteo_b.rdz_opa, dane_meteo_b.uslon, dane_meteo_b.data_tb 
FROM pozary LEFT JOIN dane_meteo ON data_txt = data_t 
LEFT JOIN dane_meteo_b ON data_txt = data_tb);"""

# zapytanie tworzące tabelę zawierającą wybrane dane pożarowe oraz meteorologiczne

query3 = """ CREATE TABLE pozar_meteo AS (
SELECT pozary.*, dane_meteo.v_wiatru, dane_meteo.temp, dane_meteo.wilg, dane_meteo_b.opad, dane_meteo_b.rdz_opa, dane_meteo_b.uslon
FROM pozary LEFT JOIN dane_meteo  ON data_txt = data_t 
LEFT JOIN dane_meteo_b ON data_txt = data_tb
);"""

# ustawienie kolumny 'wyroznik' jako klucza glownego tabeli 

query4 = """ALTER TABLE pozar_meteo
ADD PRIMARY KEY (wyroznik);""" 

try:
    eng_connect.execute(query2)
    eng_connect.execute(query3)
    print("Zapytanie zostało wykonane...")
except:
    print("Ups...")    

try:
    eng_connect.execute(query4)
    print("Zapytanie zostało wykonane...")
except:
    print("Ups...")    

# zamknięcie połączenia z bazą

eng_connect.close()
