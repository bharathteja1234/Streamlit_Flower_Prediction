import streamlit as st
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier

st.set_page_config(page_title="Prediction App",
                   layout='wide',
                   initial_sidebar_state='expanded')

st.title("Flower Species Prediction :smile:")

df = pd.read_csv("https://raw.githubusercontent.com/dataprofessor/data/master/iris.csv")

st.sidebar.subheader("Input Features")
sepal_length = st.sidebar.slider('Sepal Length', 4.3, 7.9, 5.8)
sepal_width = st.sidebar.slider('Sepal Width', 2.0, 4.4, 3.1)
petal_length = st.sidebar.slider('Petal Length', 1.0, 6.9, 3.8)
petal_width = st.sidebar.slider('Petal Width', 0.1, 2.9, 5.2)

x = df.drop('Species', axis=1)
y = df['Species']

x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)

rf = RandomForestClassifier(max_depth=2, max_features=4, n_estimators=200, random_state=42)
rf.fit(x_train, y_train)

y_pred = rf.predict([[sepal_length, sepal_width, petal_length, petal_width]])

st.subheader("Brief header")
groupby_species_mean = df.groupby('Species').mean()
st.write(groupby_species_mean)
st.line_chart(groupby_species_mean.T)

input_feature = pd.DataFrame([[sepal_length, sepal_width, petal_length, petal_width]],
                              columns=['sepal_length', 'sepal_width', 'petal_length', 'petal_width'])

st.write(input_feature)

st.subheader('Output')
st.metric('Predicted class', y_pred[0], '')
