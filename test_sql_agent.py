import streamlit as st
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain_openai import ChatOpenAI
from langchain.memory import ConversationBufferMemory
from sql_prompt import sql_prompt  # 你已有的 prompt 文件
import os

# ========== 页面基础配置 ==========
st.set_page_config(
    page_title="SQL 生成助手 - 数字货币交易所",
    layout="wide",
    page_icon="📊",
)

# ========== 自定义 CSS ==========
st.markdown("""
<style>
.stTextArea textarea {
    background: #f9fafb;
    color: #111827;
    border-radius: 12px;
    border: 1px solid #cbd5e1;
    padding: 12px;
    font-size: 16px;
    box-shadow: inset 0 1px 3px rgba(0,0,0,0.1);
}

.response-box {
    background: #f3f4f6;
    color: #1f2937;
    border-radius: 12px;
    padding: 16px;
    border: 1px solid #e5e7eb;
    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    margin-top: 15px;
}

.chat-history {
    background: #f3f4f6;
    color: #1f2937;
    border-radius: 12px;
    padding: 12px;
    border: 1px solid #e5e7eb;
}

.stButton>button {
    background: linear-gradient(90deg, #06b6d4, #3b82f6);
    color: white;
    border-radius: 10px;
    padding: 0.6em 1.2em;
    font-weight: 600;
    border: none;
}
.stButton>button:hover {
    background: linear-gradient(90deg, #0ea5e9, #2563eb);
    transform: scale(1.05);
    transition: all 0.2s ease-in-out;
}
</style>
""", unsafe_allow_html=True)


# 初始化 LLM
llm = ChatOpenAI(
    model="gpt-5",  # 或 gpt-4.1，如果你开通了
    temperature=0,
    openai_api_key=os.getenv("OPENAI_API_KEY")   # 关键
)

if "memory" not in st.session_state:
    st.session_state.memory = ConversationBufferMemory(memory_key="chat_history", return_messages=True)

memory = st.session_state.memory
sql_chain = LLMChain(llm=llm, prompt=sql_prompt, memory=memory, verbose=False)

# ========== Sidebar ==========
st.sidebar.title("📚 功能说明")
st.sidebar.info(
    "这个工具可以把自然语言转成 SQL，适用于公司数据库。\n\n"
    "👉 填写模板 → 点击生成 SQL → 等待片刻即可得到结果。"
)

st.sidebar.markdown("### ⚡ 快捷示例")
examples = [
    {
        "time_period": "2025-09-01 ～ 2025-09-07",
        "analysis_object": "代理维度",
        "metrics": ["注册人数", "交易人数", "手续费"],
        "granularity": "daily",
        "filter_condition": "国家 = Japan",
        "output_headers": "日期、注册人数、交易人数、手续费",
        "sorting_rule": "按交易人数降序，保留前100",
    }
]

for i, ex in enumerate(examples):
    if st.sidebar.button(f"💡 示例 {i+1}"):
        st.session_state["example_template"] = ex

# ========== 页面标题 ==========
st.markdown("<h1 style='text-align: center; color: #38bdf8;'>SQL 生成助手</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #94a3b8;'>数字货币交易所 · 智能 SQL 转换引擎</p>", unsafe_allow_html=True)

# ========== 模板输入区 ==========
st.markdown("### 🧠 SQL 分析模板")

example = st.session_state.get("example_template", {})

with st.form("sql_form"):
    # 1️⃣ 时间周期
    time_period = st.text_input(
        "⏰ 时间周期",
        example.get("time_period", "2025-09-01 ～ 2025-10-01")
    )

    # 2️⃣ 分析对象（允许自由填写，不必下拉）
    analysis_object = st.text_input(
        "🎯 分析对象（示例：代理维度 / 所有用户 / 币对维度 ...）",
        example.get("analysis_object", "")
    )

    # 3️⃣ 指标（多选 + 用户自定义输入）
    predefined_metrics = ["注册人数", "充值人数", "交易人数", "手续费", "盈亏", "平台收入"]
    st.markdown("📊 **指标（可多选或手动添加）**")

    selected_metrics = st.multiselect(
        "从常用指标中选择：",
        predefined_metrics,
        default=example.get("metrics", ["注册人数", "交易人数"])
    )

    custom_metrics_input = st.text_input(
        "✏️ 或手动输入自定义指标（用逗号分隔）",
        ""
    )

    # 合并预定义与自定义指标
    custom_metrics = [m.strip() for m in custom_metrics_input.split(",") if m.strip()]
    all_metrics = list(set(selected_metrics + custom_metrics)) if (selected_metrics or custom_metrics) else []

    # 4️⃣ 聚合颗粒度（允许为空）
    predefined_granularity = ["daily", "weekly", "monthly"]
    # 4️⃣ 聚合颗粒度（可选 + 可自定义）
    st.markdown("📅 **聚合颗粒度（可选）**")
    st.caption("🪄 你可以直接输入或选择：daily / weekly / monthly")
    granularity = st.text_input(
        "聚合颗粒度",
        example.get("granularity", ""),
        placeholder="例如：daily / weekly / monthly / quarterly / 其他"
    )

    # 5️⃣ 过滤条件、输出表头、排序
    filter_condition = st.text_input("🔍 过滤条件", example.get("filter_condition", ""))
    output_headers = st.text_input("🧾 输出表头规范", example.get("output_headers", ""))
    sorting_rule = st.text_input("⬇️ 排序规定", example.get("sorting_rule", ""))

    # 6️⃣ 建议思路
    suggestion = st.text_area("💡 建议 agent SQL 思路（选填）", "", height=80)

    submitted = st.form_submit_button("🎯 生成 SQL")

# ========== 拼接自然语言问题并生成 SQL ==========
if submitted:
    question = f"""
时间周期：{time_period or "（未填写）"}
分析对象：{analysis_object or "（未填写）"}
指标：{', '.join(all_metrics) if all_metrics else "（未填写）"}
聚合颗粒度：{granularity or "（未填写）"}
过滤条件：{filter_condition or "（未填写）"}
输出表头规范：{output_headers or "（未填写）"}
排序规定：{sorting_rule or "（未填写）"}
{f"建议 agent SQL 思路：{suggestion}" if suggestion.strip() else ""}
"""

    st.markdown("#### 🧩 自动生成的自然语言问题")
    st.code(question, language="text")

    with st.spinner("正在生成 SQL，请稍候..."):
        response = sql_chain.run(question=question)
        st.markdown("<div class='response-box'>", unsafe_allow_html=True)
        st.code(response, language="sql")
        st.markdown("</div>", unsafe_allow_html=True)


# ========== 清除上下文 ==========
if st.button("🧹 清除上下文"):
    memory.clear()
    st.success("上下文已清除 ✅")

# ========== 对话历史 ==========
with st.expander("📜 对话历史"):
    st.markdown("<div class='chat-history'>", unsafe_allow_html=True)
    for m in memory.chat_memory.messages:
        if m.type == "human":
            st.markdown(f"🧍 **用户**：{m.content}")
        else:
            st.markdown(f"🤖 **助手**：{m.content}")
    st.markdown("</div>", unsafe_allow_html=True)
