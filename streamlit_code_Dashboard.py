# fraud_detection_fixed.py -
import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np

# Configuración
st.set_page_config(page_title="Fraud Detection Dashboard", page_icon="🛡️", layout="wide")

# Header
st.title("🛡️ AdTech Fraud Detection Dashboard")
st.markdown("**By Nicolas Zalazar | Data Analyst | Snowflake Certified**")
st.markdown("---")

# Sidebar
with st.sidebar:
    st.markdown("### 📋 About")
    st.markdown("Data Analyst specialized in fraud detection and cloud architectures")
    st.markdown("🔗 [GitHub](https://github.com/Nicolenki7) | [LinkedIn](https://linkedin.com/in/nicolas-zalazar-63340923a/)")

# Datos simulados
@st.cache_data
def load_data():
    np.random.seed(42)
    n = 500
    df = pd.DataFrame({
        'ip': [f"192.168.1.{i}" for i in range(n)],
        'total_clicks': np.random.poisson(200, n),
        'risk_score': np.random.beta(2, 5, n),
        'fraud_flag': np.random.choice([0, 1], n, p=[0.85, 0.15]),
        'country': np.random.choice(['US', 'CN', 'RU', 'BR'], n)
    })
    # Ajustar fraud_flag
    df.loc[df['fraud_flag'] == 1, 'risk_score'] += 0.3
    return df

df = load_data()

# Métricas
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total IPs", len(df))
with col2:
    st.metric("Fraud Rate", f"{df['fraud_flag'].mean():.1%}")
with col3:
    st.metric("Avg Risk", f"{df['risk_score'].mean():.2f}")

# Gráficos
col1, col2 = st.columns(2)

with col1:
    fig = px.scatter(df, x='total_clicks', y='risk_score', color='fraud_flag',
                     title='Risk vs Clicks')
    st.plotly_chart(fig, use_container_width=True)

with col2:
    country_fraud = df.groupby('country')['fraud_flag'].mean() * 100
    fig2 = px.bar(x=country_fraud.index, y=country_fraud.values,
                  title='Fraud Rate by Country')
    st.plotly_chart(fig2, use_container_width=True)

# Footer
st.markdown("---")
st.markdown("**🚀 Real project processing 10TB+ with Snowflake and Python UDFs**")
st.markdown("*Dashboard by Nicolas Zalazar | Data Analyst*")
