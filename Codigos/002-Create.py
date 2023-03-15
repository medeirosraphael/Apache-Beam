import apache_beam as beam

p1 = beam.Pipeline()

p1 | "Tupla" >> beam.Create( [ ("Raphael",32) , ("Thainara",29) ] ) | "print Tupla" >> beam.Map(print) #tupla
p1 | "Lista" >> beam.Create ( [ 1,2,3 ] ) |  "print Lista" >> beam.Map(print) #lista

p1.run()