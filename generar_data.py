import os
import pandas as pd
import random
import faker

# se inicializa faker
fake = faker.Faker()

# 2 datos originales
data = {
    'nombre': ['Macbook Air M1', 'Lenovo ThinkPad'],
    'precio': [759.000, 5000.0],
    'correo': ['pipemendezri@gmail.com', 'otrocorreo@example.com']
}

# se crea el dataframe inicial
df = pd.DataFrame(data)

# generar 498 filas mas
new_data = []
for _ in range(498):
    name = fake.company()
    price = round(random.uniform(100, 5000), 2)
    email = fake.email()
    new_data.append({'nombre': name, 'precio': price, 'correo': email})

# convertir la lista de diccionarios en un DataFrame
new_df = pd.DataFrame(new_data)

# concatenar el DataFrame original con el nuevo DataFrame
df = pd.concat([df, new_df], ignore_index=True)

# Directorio de destino
directory = r'C:\Users\pipe_\OneDrive\Documentos\Sistemas Distribuidos\Tarea2'
# Guardar CSV en el directorio especificado
df.to_csv(os.path.join(directory, '500pedidos.csv'), index=False)

df.head()
