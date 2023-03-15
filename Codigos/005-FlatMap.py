import apache_beam as beam

p1 = beam.Pipeline()

file_input = r"C:\Users\Medeiros\Documents\GitHub\Apache-Beam\Dados\poema.txt"
file_output = r"C:\Users\Medeiros\Documents\GitHub\Apache-Beam\Saida\resultado.txt"

Collection = (
    p1
    |beam.io.ReadFromText(file_input)
    |beam.FlatMap(lambda record: record.split(' '))
    |beam.io.WriteToText(file_output)
)
p1.run()

print("Sucess Finished!")