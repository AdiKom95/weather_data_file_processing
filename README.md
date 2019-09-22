# Przetwarzanie pliku z danymi meteorologicznymi


Skrypt ma za zadanie w odpowiedni sposób przetworzyć dane meteorologiczne - wybierając z plików .csv interesujące parametry pogodowe, następnie
przekazać je do bazy danych i dołączyć do pliku zawierajacego informacje o pożarach.
Główne składowe skryptu:
    
1. Przygotowanie srodowiska pracy
   (ładowanie bibliotek, ustalanie katalogu roboczego)

2. Selekcja danych 
   (wybór plików .csv z danymi meteorologicznymi)

3. Załadowanie danych do pamięci i ich wstępne przetwarzanie

4. Przekształcenie danych do postaci DataFrame, uzupełnianie braków danych, przetwarzanie do formy docelowej - umożliwiającej eksport do bazy danych

5. Obsługa bazy danych
   (Nawiązanie połączenia z bazą danych, eksport danych do SZBD Postgres, dolączenie danych meteorologicznych do danych o pożarach)
         
   
## Środowisko pracy

* Windows 7
* Anaconda -> IDE Spyder
* Python 3.6.6 -> Pakiety: os, pandas, numpy, psycopg2, sqlalchemy
