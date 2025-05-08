import streamlit as st

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
        'การไฟฟ้านครหลวง', 'การประปานครหลวง', 'การรถไฟฟ้าขนส่งมวลชนแห่งประเทศไทย', 'กรมทางหลวง',
        'กรมทางหลวงชนบท', 'กรมชลประทาน', 'กรมป้องกันและบรรเทาสาธารณภัย', 'กรมส่งเสริมการปกครองท้องถิ่น',
        'กรมส่งเสริมคุณภาพสิ่งแวดล้อม', 'กรมควบคุมโรค', 'กรมอนามัย', 'กรุงเทพมหานคร'
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
        st.success(f"Thanks, we've received your message!")
        st.info(f"📧 This case has been received.")