import risingwave
from risingwave import RisingWaveConnection


client = RisingWaveConnection.connect("localhost", 4567)  


create_mv_query = """
CREATE MATERIALIZED VIEW prediction_data AS
SELECT
    c.jedinstveni_identifikator, 
    c.datum_registracije, 
    c.starost, 
    c.pol, 
    c.bracno_stanje, 
    c.poslednja_posjeta,
    p.pozajmica,
    p.kredit,
    p.elektronsko_bankarstvo,
    p.kreditna_kartica,
    t.isplate_i_uplate_salter_broj,
    t.isplate_i_uplate_salter_iznos,
    t.isplate_i_uplate_bankomat_broj,
    t.isplate_i_uplate_bankomat_iznos,
    t.nalozi_salter_broj,
    t.nalozi_salter_iznos,
    t.nalozi_devizni_salter_broj,
    t.nalozi_devizni_salter_iznos,
    t.nalozi_ebankarstvo_broj,
    t.nalozi_ebankarstvo_iznos,
    t.nalozi_smartatm_broj,
    t.nalozi_smartatm_iznos,
    t.trajni_nalog_broj,
    t.trajni_nalog_iznos,
    t.placanje_i_isplata_kreditna_kartica_broj,
    t.placanje_i_isplata_kreditna_kartica_iznos,
    t.placanje_kreditna_kartica_web_broj,
    t.placanje_kreditna_kartica_web_iznos
FROM
    client_latest_mv c
LEFT JOIN
    product_window_summary p ON c.jedinstveni_identifikator = p.jedinstveni_identifikator
LEFT JOIN
    transaction_window_summary t ON c.jedinstveni_identifikator = t.jedinstveni_identifikator;
"""

client.execute(create_mv_query)
print("Materijalizovani pogled 'prediction_data' je uspešno kreiran.")
