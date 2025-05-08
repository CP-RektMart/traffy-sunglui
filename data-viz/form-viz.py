import streamlit as st
import requests
from datetime import timedelta

st.title("Form")

with st.form(key='input_form'):
    comment = st.text_area("Comment", "Type your comment here...")
    types = [
        'PM2.5', 'การเดินทาง', 'กีดขวาง', 'คนจรจัด', 'คลอง', 'ความปลอดภัย', 'ความสะอาด',
        'จราจร', 'ต้นไม้', 'ถนน', 'ทางเท้า', 'ท่อระบายน้ำ', 'น้ำท่วม', 'ป้าย', 'ป้ายจราจร',
        'ร้องเรียน', 'สอบถาม', 'สะพาน', 'สัตว์จรจัด', 'สายไฟ', 'ห้องน้ำ', 'เสนอแนะ',
        'เสียงรบกวน', 'แสงสว่าง'
    ]

    organizations = [
        'สำนักงบประมาณกรุงเทพมหานคร', 'สำนักป้องกันและบรรเทาสาธารณภัย กทม.', 'สำนักการจราจรและขนส่ง กรุงเทพมหานคร (สจส. กทม.)', 
        'สำนักการโยธา กทม.',
        'สำนักการระบายน้ำ กทม.', 'สำนักเทศกิจ กทม.', 'สำนักวัฒนธรรม กีฬาและการท่องเที่ยว กทม.', 'สำนักพัฒนาสังคม กทม.',
        'สำนักการวางผังและพัฒนาเมือง กทม.', 'สำนักการแพทย์ กทม.', 'สำนักสิ่งแวดล้อม กทม.', 'สำนักการศึกษา สนศ.กทม.'
    ]

    st.subheader("Type of case")
    selected_types = []
    cols = st.columns(4) 
    for i, t in enumerate(types):
        col = cols[i % 4]  
        checkbox_label = f"{t}"
        if col.checkbox(checkbox_label, value=False, key=f"type_{t}"):
            selected_types.append(t)

    st.subheader("Organization to be reported to")
    selected_orgs = []
    cols = st.columns(4)
    for i, org in enumerate(organizations):
        col = cols[i % 4]  
        checkbox_label = f"{org}"
        if col.checkbox(checkbox_label, value=False, key=f"org_{org}"):
            selected_orgs.append(org)

    submitted = st.form_submit_button("Submit")
    
    if submitted:
        request_body = {
            "types": selected_types,
            "organizations": selected_orgs
        }
        response = requests.post("http://127.0.0.1:8000/models/predict", json=request_body)

        if response.status_code == 200:
            st.success("Report submitted successfully.")
        else:
            st.error("Failed to submit the report.")
            
        st.write("Prediction for type:", (i for i in selected_types), "and organization:", (i for i in selected_orgs))
        no_of_hrs = response.json()["prediction"][0]
        delta = timedelta(hours=no_of_hrs)
        highlighted_text = f'''
        <div style="background-color:#333;padding:10px;border-radius:5px;color:white;">
            Issue to be solved in : <span style="color:lightgreen;font-weight:bold;">{delta.days} days</span>
        </div>
        '''
        st.markdown(highlighted_text, unsafe_allow_html=True)
        