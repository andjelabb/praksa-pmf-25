import time
import random
from random import randrange
from datetime import datetime, timedelta


def generisi_starost():
    return random.randint(18, 75)

def generisi_pol():
    return random.choice(["Muski", "Zenski"])

def generisi_bracno_stanje():
    return random.choice(["Samac", "U braku", "Razveden", "Udovac"])

brojac = 0
def generisi_identifikator():
    global brojac
    brojac += 1
    return brojac -1
    

def generisi_datum_registracije():
    return datetime.now() #- timedelta(days=random.randint(0, 365))

def vrati_id_klijenta(klijent):
    return klijent["jedinstveni_identifikator"]


def generisi_klijenta():
    klijent = {
        "datum_registracije": str(generisi_datum_registracije().strftime("%Y-%m-%d %H:%M:%S")),
        "jedinstveni_identifikator": generisi_identifikator(),
        "starost": generisi_starost(),
        "pol": generisi_pol(),
        "bracno_stanje": generisi_bracno_stanje()
    }
    return klijent












    
    



