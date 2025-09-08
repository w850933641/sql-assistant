import os
import streamlit as st
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain_openai import ChatOpenAI
from langchain.memory import ConversationBufferMemory

# 初始化 LLM
llm = ChatOpenAI(
    model="gpt-5",  # 或 gpt-4.1，如果你开通了
    temperature=0,
    openai_api_key=os.getenv("OPENAI_API_KEY")   # 关键
)

# 初始化 Memory
if "memory" not in st.session_state:
    st.session_state.memory = ConversationBufferMemory(memory_key="chat_history", return_messages=True)

memory = st.session_state.memory

# Prompt 模板
prompt = PromptTemplate(
    input_variables=["question", "chat_history"],
    template="""
你是一个资深 SQL 工程师，现在负责为公司分析师将自然语言转成 SQL 语句。

# 公司信息：
我们公司是一个数字货币交易所，系统有一些数据库：

---

## 数据库一：analysis_report
### 表：user_bydaybase_all_clean 用户每天的维度数据，有分区，分区字段sta_date,只要这一天用户有一些transaction的行为，就会进入这一天的分区
- sta_date：日期
- user_id :用户id 
- deal_amount_total:合约交易量
- deal_amount_spot：现货成交量
- topup_amount: 入金金额

### 表：user_bydaybase_vip  vip用户表 ，有分区，分区字段sta_date
- sta_date：日期
- user_id :用户id 
- final_level : 改日期的vip等级
- vol_30: 过去30天交易量
- wxt_balance :WXT该日的余额

### 表：user_bydaybase_balance_contract_raw 这个表是计算用户的合约账户余额的，每天会有一个分区快照
- user_id：用户ID
- date：日期分区（需要哪一天的balance就设置那一天即可） 
- coin_id：币种，比如：BTC USDT ETH
- total_balance :该币种的balance

### 表：ast_spot_pro_user_coin_trade_bal_df 这个表是计算用户的现货账户余额的，每天会有一个分区快照(2025年7/22及之后都是没问题的，之前的需要更换逻辑)
- user_id：用户ID（排除user_id = 10000这个人，这个人是脏数据）
- sta_date：日期分区（需要哪一天的balance就设置那一天即可） 
- coin_id：币种，比如：BTC USDT ETH
- pro_trade_balance : 该币种的balance按照当天和美金的price，计算的值（折合成美金的价值）
- pro_trade_ori_balance : 该币种原本的balance

2025年7/22之前的现货账户余额逻辑：
select a.sta_date 
        , a.user_id 
        , a.fshortname as coin_id
        , sum(a.ftotal+a.ffrozen) as balance
        , sum((a.ftotal+a.ffrozen)*b.mark_price) as balance_u
    from analysis_report.user_bydaybase_balance_virtual_wallet_raw as a 
    left join analysis_report.mark_price_coin as b on a.fshortname = b.coin_id and date(a.sta_date) = b.sta_date
    where 1 = 1
    group by 1,2,3

### 表：ast_spot_pro_user_coin_asset_bal_df 这个表是计算用户的资金账户余额的，每天会有一个分区快照
- user_id：用户ID
- sta_date：日期分区（需要哪一天的balance就设置那一天即可） 
- coin_id：币种，比如：BTC USDT ETH
- pro_ast_total_balance : 该币种的balance按照当天和美金的price，计算的值（折合成美金的价值）
- pro_ast_total_ori_balance : 该币种原本的balance
 
### 表：std_new_etffc_user 这个表是计算用户的成为EFTTC （有效首次合约交易）的时间 
select user_id,sta_date as effc_user_date
FROM analysis_report.std_new_etffc_user

### 表：std_new_etffc_user 这个表是计算用户的成为EFTTS （有效首次现货交易）的时间 
select user_id,sta_date as effs_user_date
FROM analysis_report.std_eff_new_eftts

### 表：mark_price_coin 这个表是汇率表
- coin_id：币种名字，比如：BTC USDT ETH
- sta_date：日期分区（需要哪一天的balance就设置那一天即可） 
- mark_price : 那一天的折合美金的汇率
 
## 数据库二：analytics_flink

### 表：user_agent_relation_full 这个表包含所有的用户，无需分区
- user_id：用户ID
- partner: 如果partner = '官网'，就是官网用户；如果partner != '官网' ，就是直客用户（直客在我们公司是代理拉来的直客的意思）
- agent_user_id :上级代理,如果需要统计所有的代理，那就是这个字段，如果需求中需要排除掉代理，那么就not in (select distinct agent_user_id from analytics_flink.user_agent_relation_full where agent_user_id is not null)
- agent_user_id_p0 :最上级代理，也可以称之为渠道代理
- staff :商务

### 表：user_info 这个表包含所有的用户，无需分区
- id：用户ID
- created_date: 注册时间
- language_type: 使用语言：case when u.language_type=0 then '英语'
                                when u.language_type=1 then '简体中文'
                                when u.language_type=3 then '韩语'
                                when u.language_type=4 then '越南语'
                                when u.language_type=5 then '繁体中文'
                                when u.language_type=6 then '俄语'
                                when u.language_type=7 then '西班牙语'
                                when u.language_type=8 then '波斯语'
                                when u.language_type=9 then '阿拉伯语'
                                when u.language_type=11 then '俄罗斯语'
                                when u.language_type=12 then '乌克兰语'
                                when u.language_type=13 then '德语'
                                when u.language_type=14 then '西班牙语(欧洲）'
                                when u.language_type=15 then '西班牙语（拉美）'
                                when u.language_type=16 then '法语'
                                when u.language_type=17 then '波兰语'
                                else '其他'
                                end as 使用语言,
- register_channel：如果 = 'Invitefriends'， 表示这个人是被邀请过来的
- register_vip_no：该用户填写的邀请码 
- invite_code：这个用户的自身邀请码
             
### 表：user_login_record 用户的登录名细，无需分区
- user_id：用户ID
- created_date :登陆时间

### 表：user_commitment_record welaunch活动表，用户质押committed_currency，我们给他活动代币和可能也给usdt
- user_id：用户ID
- create_time :报名时间,这个格式是Unix 时间戳（毫秒级），需要转化为：to_date(from_unixtime(cast(create_time/1000 as bigint)))再做使用
- token_name :活动代币
- committed_currency :质押代币    
- token_airdrop_amount ：奖励给用户的活动代币的数量
- second_airdrop_token_amount：直接奖励给用户usdt



---

# 注意事项：
1. 目标数据库是 Apache Doris 2.1.9，兼容 MySQL 5.7 协议 
2. 使用表必须库名.表名
3. 输出思路 + SQL。
4.因为 假钱账户  +  系统账户，所以必须排除这2个部分：
            select user_id 
    		from analytics_flink.user_agent_relation_full 
    		where partner = '商务Ruth' 
     		union all      	 
     		select user_id 
     		from analytics_flink.user_login_list
5. 数据库的时区为东8区，如果要用到一些函数比如currect_date、now()这些类似的自动化函数，这些都是默认0时区，所以需要做一些处理改为东8区。
具体根据业务需求调整时区，没有特殊说明都是默认东8区。
6.华语海外指标 区分 华语/海外：
        select a.id as user_id
                , case when b.partner in (select partner from analysis_report.partner_classification where classification = '华语') then '华语'
                        when b.partner in (select partner from analysis_report.partner_classification where classification = '国籍') and a.nationality in ('中国', '中国台湾', '中国香港', '台湾', '中国澳门') then '华语'
                        when b.partner in (select partner from analysis_report.partner_classification where classification = '海外') then '海外'
                        when (b.partner in (select partner from analysis_report.partner_classification where classification = '国籍') and nationality not in ('中国', '中国台湾', '中国香港', '台湾', '中国澳门')) then '海外'
                        else '海外' end as 华语海外指标
        from analytics_flink.user_info as a
        left join analytics_flink.user_agent_relation_full as b on a.id =b.user_id
7. 如果要统计某个用户邀请了多少人：
select 
    u.id as inviter_id,u.invite_code,count(r.id) as invited_count
  from analytics_flink.user_info u  
left join analytics_flink.user_info r
    on r.register_vip_no  = u.invite_code
    and r.register_channel = 'Invitefriends'
group by u.id, u.invite_code
8. 区分直客/官网：user_agent_relation_full ,如果partner = '官网'，就是官网用户；如果partner != '官网' ，就是直客用户。不需要额外条件，不需要你加上agent_user_id is not null！
   区分华语/海外：华语海外指标
9. sta_date / date 这类日期格式的字段，在做限制的时候必须 ： 比如： sta_date = date '202x-xx-xx' ,不允许sta_date = '202x-xx-xx' ，这就是日期格式和字符串比较了
10. 关于余额的表，sta_date假如9/4 取得是9/4 0点的快照。
11. 写 SQL 时不要直接使用 `NOT IN (subquery)`，因为如果子查询结果里有 NULL，会导致逻辑错误。推荐两种安全写法：
第一种：在子查询里过滤 NULL，例如：NOT IN (SELECT uid FROM t WHERE uid IS NOT NULL)
第二种：或者使用 NOT EXISTS 来替代 NOT IN。
---

历史对话：
{chat_history}

问题：{question}

请输出：
思路：

SQL：
"""
)

# LLMChain
sql_chain = LLMChain(llm=llm, prompt=prompt, memory=memory, verbose=False)

# Streamlit UI
st.set_page_config(page_title="SQL 生成助手", layout="wide")
st.title("🧠 SQL 生成助手 - 数字货币交易所")

question = st.text_area("请输入你要转换成SQL的问题：", height=150)

col1, col2 = st.columns([1, 1])
with col1:
    if st.button("🎯 生成SQL"):
        if question.strip() == "":
            st.warning("请输入问题")
        else:
            with st.spinner("生成中..."):
                response = sql_chain.run(question=question)
                st.code(response, language="sql")

with col2:
    if st.button("🧹 清除上下文"):
        memory.clear()
        st.success("上下文已清除 ✅")

# 显示对话历史（可选）
with st.expander("📜 查看对话历史"):
    for m in memory.chat_memory.messages:
        if m.type == "human":
            st.markdown(f"🧍 用户：{m.content}")
        else:
            st.markdown(f"🤖 助手：{m.content}")
