import apache_beam as beam

file_input = r"C:\Users\Medeiros\Documents\GitHub\Apache-Beam\Dados\voos_sample.csv"

p1 = beam.Pipeline()

voos = (
p1
  | "Importar Dados" >> beam.io.ReadFromText(file_input, skip_header_lines = 1)
  | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos de Los Angeles" >> beam.Filter(lambda record: record[3] == "LAX")
  | "Mostrar Resultados" >> beam.Map(print)
)

p1.run()

print("Sucess Finished!")