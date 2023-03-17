import apache_beam as beam

palavras=['quatro','um']
file_input = r"C:\Users\Medeiros\Documents\GitHub\Apache-Beam\Dados\poema.txt"
file_output = r"C:\Users\Medeiros\Documents\GitHub\Apache-Beam\Saida\resultado_poema.txt"

def encontrarPalavras( i ):
 if i in palavras:
    return True

p1 = beam.Pipeline()

Collection = (
    p1
    |beam.io.ReadFromText(file_input)
    |beam.FlatMap(lambda record: record.split(' '))
    |beam.Filter(encontrarPalavras)
    |beam.io.WriteToText(file_output)
)
p1.run()

print("Sucess Finished!")