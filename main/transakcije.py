import random
from datetime import datetime, timedelta


transaction_types = [
    "isplate i uplate šalter", 
    "isplate i uplate bankomat", 
    "nalozi šalter", 
    "nalozi devizni šalter", 
    "nalozi e-bankarstvo", 
    "nalozi smart-atm", 
    "trajni nalog", 
    "plaćanje i isplata kreditna kartica", 
    "plaćanje kreditna kartica web"
]

def generisi_datum_transakcije():
    return datetime.now() #- timedelta(days=random.randint(0, 365))


def generisi_iznos():
    return round(random.uniform(10, 5000), 2)


def generisi_tip_transakcije():
    return random.choice(transaction_types)


def generisi_transakciju(klijent_id):
    transakcija= {
        "jedinstveni_identifikator": klijent_id,
        "datum_transakcije": str(generisi_datum_transakcije().strftime("%Y-%m-%d %H:%M:%S")),
        "vrsta_transakcije": generisi_tip_transakcije(),
        "iznos_transakcije": generisi_iznos()
    }
    
    return transakcija




