import streamlit as st
mysql = {
	'hostname': st.secrets["database"]["host"],
	'port': st.secrets["database"]["port"],
	'user': st.secrets["database"]["user"],
	'password': st.secrets["database"]["password"],
	'target_database': st.secrets["database"]["dbname"]
}