import time
import random
from klijent import generisi_klijenta, vrati_id_klijenta
from bankarski_proizvod import generisi_proizvode
from utils.minio_utils.kafka_utils import create_kafka_producer, send_data_to_topic
from transakcije import generisi_transakciju

def main():
    klijenti_podaci = {}
    producer = create_kafka_producer()

    #pocetna vjerovatnoca za generisanje novih klijenata
    pocetna_verovatnoca = 0.9    #velika vjerovatnoca za nove klijente na pocetku
    trenutna_verovatnoca = pocetna_verovatnoca
    iteracija = 0

    while True:
        iteracija += 1
        
        #nakon 100 iteracija smanjujem vjerovatnoca za nove klijente
        if iteracija > 10: 
            trenutna_verovatnoca = max(0.1, trenutna_verovatnoca - 0.01)

        #na osnovu trenutne vjerovatnoce odlucujem da li cu generisati novog klijenta
        if random.random() < trenutna_verovatnoca:
            klijent = generisi_klijenta()
            id_klijenta = vrati_id_klijenta(klijent)
            transakcija = generisi_transakciju(id_klijenta)

        
            send_data_to_topic(producer, "client_events", klijent)
            klijenti_podaci[id_klijenta] = {'bankarski_proizvodi': []}

        else:
            #ako nemam novog klijenta generisem samo transakciju za postojeceg
            if klijenti_podaci:
                id_klijenta = random.choice(list(klijenti_podaci.keys()))
                transakcija = generisi_transakciju(id_klijenta)

                send_data_to_topic(producer, "transaction_events", transakcija)

            
            if random.random() < 0.9:  #dodala sam 10% sanse za promjenu bankarskog proizvoda za postojece klijente
                id_klijenta = random.choice(list(klijenti_podaci.keys()))
                bankarski_proizvod = generisi_proizvode(id_klijenta)
                klijenti_podaci[id_klijenta]['bankarski_proizvodi'].append(bankarski_proizvod)
                
                send_data_to_topic(producer, "product_change_events", bankarski_proizvod)

        
        time.sleep(2)

if __name__ == "__main__":
    main()






