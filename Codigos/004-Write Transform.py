import apache_beam as beam

p1 = beam.Pipeline()

file_input = r"C:\Users\Medeiros\Documents\GitHub\Apache-Beam\Dados\voos_sample.csv"
file_output = r"C:\Users\Medeiros\Documents\GitHub\Apache-Beam\Saida\voos.txt"

voos = (
p1
  | "Importar Dados" >> beam.io.ReadFromText(file_input, skip_header_lines = 1)
  | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
  | "Gravar Resultado" >> beam.io.WriteToText(file_output)
)
p1.run()
