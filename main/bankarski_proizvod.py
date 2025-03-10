import time
import random
from datetime import datetime, timedelta


bankarski_proizvodi = ['pozajmica', 'kredit', 'elektronsko bankarstvo', 'kreditna kartica']

def generisi_datum():
    return datetime.now() #- timedelta(days=random.randint(0, 365))


def generisi_promjenu():
    return random.choice(['uzmi', 'otkaži'])


def generisi_proizvode(klijent_id):
    bankarski_proizvod= {
        "jedinstveni_identifikator": klijent_id,
        "datum": str(generisi_datum().strftime("%Y-%m-%d %H:%M:%S")),
        "proizvod": random.choice(bankarski_proizvodi),
        "indikator": generisi_promjenu()
    }
    return bankarski_proizvod


