from load_baci_to_bigquery import get_client, load_countries, load_products

client = get_client()
load_countries(client)
load_products(client)
