# fraud_detection_fixed.py - VERSIÓN STREAMLIT CLOUD
import streamlit as st
import pandas as pd
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

# Gráficos con STREAMLIT NATIVO (sin Plotly)
col1, col2 = st.columns(2)

with col1:
    st.subheader("📊 Distribución de Riesgo")
    # Histograma con Streamlit
    hist_values, hist_bins = np.histogram(df['risk_score'], bins=20)
    st.bar_chart(hist_values)

with col2:
    st.subheader("🌍 Fraude por País")
    country_fraud = df.groupby('country')['fraud_flag'].mean() * 100
    st.bar_chart(country_fraud)

# Tabla interactiva
st.subheader("📋 Datos Detallados")
st.dataframe(df.head(50))

# Footer
st.markdown("---")
st.markdown("**🚀 Real project processing 10TB+ with Snowflake and Python UDFs**")
st.markdown("*Dashboard by Nicolas Zalazar | Data Analyst*")
