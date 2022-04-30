from pycelonis import get_celonis

celonis = get_celonis(
    url="https://academic-aravind-sankaran-rwth-aachen-de.eu-2.celonis.cloud",
    api_token = "OWIyODI0ZTYtZDYyMS00N2QwLTk1ZTEtN2ZhN2U3NGYzYjg0Okp2a3FQeUNNanovVjM0K0ZHTVNpbTJPQWt1Y2RtZGdINlR2THRDbHd3Y1By"
)

print(celonis.datamodels)