import apache_beam as beam

# definir pipeline
p1 = beam.Pipeline()

file_path = r"C:\Users\Medeiros\Documents\GitHub\Apache-Beam\Dados\voos_sample.csv"

voos = (
p1
  # ler arquivo, e excluir o cabeçalho
  # as pipes significam que um comando é usado como input do outro
  | "Importar Dados" >> beam.io.ReadFromText(file_path, skip_header_lines = 1)
  | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
  | "Mostrar Resultados" >> beam.Map(print)
)

# comando para executar
p1.run()