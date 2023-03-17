import apache_beam as beam

file_input = r"C:\Users\Medeiros\Documents\GitHub\Apache-Beam\Dados\voos_sample.csv"

p1 = beam.Pipeline()

Tempo_Atrasos = (
p1
  | "Importar Dados" >> beam.io.ReadFromText(file_input)
  | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos de Los Angeles" >> beam.Filter(lambda record: int(record[8]) > 0 )
  | "Criar par" >> beam.Map(lambda record: (record[4],int(record[8])))
  | "Somar por key" >> beam.CombinePerKey(sum)
  | "Mostrar Resultados" >> beam.Map(print)
)

p1.run()