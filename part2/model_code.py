#%%
import pandas as pd
import numpy as np
import tensorflow as tf
import tensorflow.keras as K
from tensorflow.keras import layers
from tensorflow.python.eager.function import defun_with_attributes
tf.__version__

#%%
df = pd.read_csv('train_df.csv')
df.head()
df.shape


#%%
df_train_x = df.drop(['value', 'item_id'], axis = 1).copy()
df_train_y = df['value'].copy()
df_log_y = np.log(df_train_y)

#%%
df_x = (df_train_x - df_train_x.mean())/df_train_x.std()
df_time = df_x['time_numeric'].copy()
df_without_time = df_x.drop(['time_numeric'], axis = 1).copy()

#%%
input1 = layers.Input(shape = (1, ))
sin_1 = layers.Dense(10, activation = 'linear')(input1)
cos_1 = layers.Dense(10, activation = 'linear')(input1)

input2 = layers.Input(shape = (len(df_without_time.columns), ))

layer1 = tf.concat([tf.sin(sin_1), tf.cos(cos_1), input2], axis = 1)

Dense_2 = layers.Dense(100, activation = 'swish')(layer1) #100, 70, 50
Dense_3 = layers.Dense(100, activation = 'swish')(Dense_2)
Dense_4 = layers.Dense(100, activation = 'swish')(Dense_3)
output = layers.Dense(1, activation = 'linear')(Dense_4)

model = K.Model([input1, input2], output)

model.summary()

model.compile(optimizer=K.optimizers.RMSprop(0.0005), 
              loss= K.losses.MeanAbsolutePercentageError(),
              metrics=['mae', 'mape'])

model.fit(x = [df_time, df_without_time], y = df_log_y, batch_size = 100, epochs = 50)
#%%
y_predict = model.predict(x = [df_time, df_without_time])
y_predict
df_log_y

np.exp(y_predict)
df_train_y


#%%
df_test = pd.read_csv('test_df.csv')

#%%
df_test_x = df_test.drop(['item_id'], axis = 1).copy()
df_test_x = (df_test_x - df_train_x.mean())/df_train_x.std()
df_test_x

df_test_time = df_test_x['time_numeric'].copy()
df_test_without_time = df_test_x.drop(['time_numeric'], axis = 1).copy()
y_test_predict = model.predict(x = [df_test_time, df_test_without_time])

y_predicted = pd.concat([df_test['item_id'], pd.DataFrame(np.exp(y_test_predict), columns = ['value'])], axis = 1)
y_predicted.to_csv('predicted.csv')

df_test_x.columns
df_train_x.columns




#%%
# class BuildModel(K.models.Model):
#     def __init__(self, name = 'ValueEstimate', **kwargs):
#         super(BuildModel, self).__init__(name=name, **kwargs)
#         self.d1 = layers.Dense(10, activation = 'relu')
#         self.d2 = layers.Dense(10, activation = 'relu')
#         self.d3 = layers.Dense(1, activation = 'linear')
        
#     def time_embedding(x):
#         tmp = layers.Dense(2, activation = 'linear')(x)
#         return(tf.concat(tf.cos(tmp), tf.sin(tmp)))
        
#     def call(self, x1, x2):
#         x_time = self.time_embedding(x1)
#         x = tf.concat(x2, x_time)
#         x = self.d1(x)
#         x = self.d2(x)
#         y = self.d3(x)
#         return y
# model = BuildModel()
# model.compile(optimizer='adam', loss= tf.keras.losses.MeanAbsoluteError(),
#               metrics=[tf.keras.losses.MeanAbsoluteError()])
# model.fit(x = [df_time, df_without_time], y = df_y, batch_size = 100)