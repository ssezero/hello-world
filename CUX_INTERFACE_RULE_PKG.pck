create or replace package CUX_INTERFACE_RULE_PKG is

  -- Author  : NEAL
  -- Created : 2018/3/19 13:03:38
  -- Purpose : 该package是为通用接口平台生成会计规则使用。
  

Procedure main( ERRBUF  OUT Varchar2,RETCODE  OUT VARCHAR2,p_ledger In Number,p_detial In Number,P_batch    In Varchar2 Default 2,p_user     In Varchar2);
Function get_rule_result(p_group_id In Number,P_deatal In Number) Return Varchar2;
Function f_Isdate(p_Colvalue  Varchar2,
                                    p_Mandatory Varchar2)
  Return Pls_Integer;
 Function  create_gl_record(p_ledger_id In Number) Return Varchar2;
 Function validate_ic(p_value Varchar2) Return Varchar2;
  Procedure update_attribute_header(p_ledger_id In Number);
    Function get_ic(p_value  Varchar2,p_mapp Varchar2) Return Varchar2;
      Function get_bankflow_source_type (p_detial_id In Varchar2) Return Varchar2;
       Function get_mapping_comments(p_value  Varchar2,p_mapp Varchar2) Return Varchar2;
end CUX_INTERFACE_RULE_PKG;
/
create or replace package body CUX_INTERFACE_RULE_PKG is
/*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                get_rule_result
  *   DESCRIPTION:
  *                规则
  *
  *   HISTORY:
  *     1.00   2017-12-15   sun.zheng   Creation
  *
  * ==============================================*/
---取组规则程序 
  const_ledger Number;
  G_PKG_NAME CONSTANT VARCHAR2(35) := 'CUX_INTERFACE_RULE_PKG';
  const_batch_id  VARCHAR2(500);
Function get_rule_result(p_group_id In Number,P_deatal In Number) Return
  Varchar2
  Is 
  l_return Varchar2(500);
  L_gropu_id Varchar2(200);
  L_COM    Varchar2(500);
  L_var    Varchar2(500);
  cv Sys_Refcursor;  ---动态游标
  l_sql Varchar2(4000);
  L_DATA_VALUES Varchar2(500);
  L_deatal Varchar2(20):=160;
  ---取组下有多少行
  Cursor cur_get Is 
  Select Card.Group_Id,
       Card.Sequences,
       card.header_id,
       card.line_type_id,
       Card.Sources,
       Card.Data_Column,
       Card.Data_Values,
       Card.Operators,
       card.functioned
  From Cux_Ass_Rule_Detail_All Card ---规则明细
 Where Card.Group_Id = p_group_id ---对应的组
 Order By card.sequences;
 Begin
   L_deatal:=P_deatal;
     For cur_fetch In cur_get Loop
       If cur_fetch.Sources = 'TABLE' And  cur_fetch.functioned Is Null  Then ---TABLE
         l_sql:= 'Select  '||cur_fetch.DATA_COLUMN||' From CUX_GL_DETAIL_ALL where DETIAL_ID='||L_deatal;
           Open cv For l_sql;
           Loop
           Fetch cv Into  L_var;
           Exit When cv%Notfound;
           End Loop;
           L_var:=Replace(L_var,'''','''''');
           L_var:=''''||L_var||'''';
          Close cv;
       Elsif cur_fetch.Sources = 'TABLE' And cur_fetch.functioned  Is Not Null Then ---TABLE+函数处理
        L_DATA_VALUES:=cur_fetch.functioned;
         l_sql:= 'Select  '||L_DATA_VALUES||' From CUX_GL_DETAIL_ALL where DETIAL_ID='||L_deatal;
           Open cv For l_sql;
           Loop
           Fetch cv Into  L_var;
           Exit When cv%Notfound;
           End Loop;
           L_var:=Replace(L_var,'''','''''');
           L_var:=''''||L_var||'''';
          Close cv;
         
       Elsif cur_fetch.Sources = 'INPUT' And cur_fetch.functioned Is Null Then 
           L_var:=''''||cur_fetch.DATA_VALUES||'''';
       Elsif cur_fetch.Sources = 'INPUT' And cur_fetch.functioned Is Not Null Then 
          l_sql:= 'Select '||cur_fetch.functioned||'  From dual';
          Open cv For l_sql;
           Loop
           Fetch cv Into  L_var;
           Exit When cv%Notfound;
           End Loop;
           L_var:=Replace(L_var,'''','''''');
           L_var:=''''||L_var||'''';
          Close cv; 
           L_var:=Replace(L_var,'''','''''');
           L_var:=''''||L_var||'''';    
       Elsif cur_fetch.Sources = 'DATASET' Then 
           L_var:=''''||cur_fetch.DATA_COLUMN||'''';
       Elsif cur_fetch.Sources = 'CALGROUP' Then 
           --当碰到组的时候有，我们需要进一步转化
           Select Carg.Group_Id Into L_gropu_id
            From Cux_Ass_Rule_Group_All Carg
           Where 1=1
                And nvl(Carg.Line_Type_Id,-1) =Nvl(cur_fetch.line_type_id,nvl(Carg.Line_Type_Id,-1))
                And carg.header_id=cur_fetch.header_id
             And Carg.Group_Name = cur_fetch.data_column;
            
           L_var:=''''||get_rule_result(L_gropu_id,L_deatal )||'''';  
             
       End If;
      
      L_COM:=L_COM||L_var||cur_fetch.Operators;--拼上操作符
    End Loop;
    L_COM:=rtrim(L_COM,'||');--排除最后出现||的情形
      ----组合完后执行
         l_sql:= 'Select '||L_COM||'  From dual';
         If L_COM Is Null Then
           cux_uninterface_pkg.log(const_ledger,P_deatal,'此组:'||p_group_id||'未定义取值规则',const_batch_id);
           Return '未定义规则！'; 
         End If;
         dbms_output.put_line('l_sql:='||l_sql);  
           Open cv For l_sql;
           Loop
           Fetch cv Into  l_return;
           Exit When cv%Notfound;
           End Loop;
          Close cv; 
   
    Return l_return;
    Exception When Others Then     
      cux_uninterface_pkg.log(const_ledger,P_deatal,'此组:'||p_group_id||'定义规则存在错误:'||Sqlerrm||'SQL:'||l_sql,const_batch_id);
    Return '存在错误:'||Sqlerrm;
 End; 
 
 /*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                get_IC
  *   DESCRIPTION:
  *                交易对象
  *
  *   HISTORY:
  *     1.00   2018-04-25   sun.zheng   Creation
  *
  * ==============================================*/  
 Function get_ic(p_value  Varchar2,p_mapp Varchar2) Return Varchar2
   Is
   l_return Varchar2(240):='';
   Begin
     Select Cml.Attribute1
       Into l_Return
       From Cux_Mapp_Lines_All Cml
           ,Cux_Mapp_Headers_All CMH
     Where Cml.Input_Value = p_Value
      And cmh.header_id=cml.header_id
      And cmh.mapping_name=p_mapp
     And ROWNUM=1
     ;
     Return l_return;
   Exception When Others Then
     Return '';
   End;
 
 
  /*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                get_mapping_comments
  *   DESCRIPTION:
  *                取映射对应的银行备注的名字，为了银行流程
  *                日记账的日记账名称使用
  *   HISTORY:
  *     1.00   2018-04-28   sun.zheng   Creation
  *
  * ==============================================*/  
 Function get_mapping_comments(p_value  Varchar2,p_mapp Varchar2) Return Varchar2
   Is
   l_return Varchar2(240):='';
   Begin
     Select Cml.Attribute2
       Into l_Return
       From Cux_Mapp_Lines_All Cml
           ,Cux_Mapp_Headers_All CMH
     Where Cml.Input_Value = p_Value
      And cmh.header_id=cml.header_id
      And cmh.mapping_name=p_mapp
     And ROWNUM=1
     ;
     Return l_return;
   Exception When Others Then
     Return '';
   End;
 
 
 /*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                get_rule_result
  *   DESCRIPTION:
  *                规则
  *
  *   HISTORY:
  *     1.00   2017-12-15   sun.zheng   Creation
  *
  * ==============================================*/
---取组规则程序 
Function validate_ic(p_value Varchar2) Return Varchar2
  Is 
  L_retun Number;
  Begin
  Select Count(1)
    Into l_Retun
    From Fnd_Flex_Value_Sets Ffvs, Fnd_Flex_Values_Vl Ffv
   Where Ffvs.Flex_Value_Set_Id = Ffv.Flex_Value_Set_Id
     And Ffvs.Flex_Value_Set_Name = '10_COA_IC'
     And Ffv.Enabled_Flag = 'Y'
     And Ffv.Summary_Flag = 'N'
     And Ffv.Description = p_Value;
    
   If  l_Retun>0 Then 
   Return 'Y';
   Else
    Return 'N';
   End If;
   Exception When Others Then 
    
     cux_uninterface_pkg.log(const_ledger,-9999,'vlidate_ic:函数存在错误'||Sqlerrm,const_batch_id);
     Return 'S';
  End;
 
 /*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                get_rule_result
  *   DESCRIPTION:
  *                规则
  *
  *   HISTORY:
  *     1.00   2017-12-15   sun.zheng   Creation
  *
  * ==============================================*/
---取组规则程序 
Function f_Isdate(p_Colvalue  Varchar2,p_Mandatory Varchar2)
  Return Pls_Integer Is
  v_Cn Pls_Integer;
Begin
  If p_Mandatory = 'yes' Then
    If p_Colvalue Is Null Then
      Return 2;
    Else
      Begin
        Select 1
          Into v_Cn
          From Dual
         Where Regexp_Like(p_Colvalue,
                           '^[1-9][[:digit:]]{3}(-|/|\.)[[:digit:]]{1,2}(-|/|\.)[[:digit:]]{1,2}$')
           And Not
                Regexp_Like(p_Colvalue,
                            '^[1-9][[:digit:]]{3}-((00|0)-((00|0)|[[:digit:]]{1,2})|[[:digit:]]{1,2}-(00|0))$')
           And Regexp_Substr(p_Colvalue, '[[:digit:]]{1,2}', 6) <= 12
           And Regexp_Substr(p_Colvalue, '[[:digit:]]{1,2}$') <= 31;
        Return 1;
      Exception
        When Others Then
          Return 0;
      End;
    End If;
  Elsif p_Mandatory = 'no' Then
    If p_Colvalue Is Null Then
      Return 1;
    Else
      Begin
        Select 1
          Into v_Cn
          From Dual
         Where Regexp_Like(p_Colvalue,
                           '^[1-9][[:digit:]]{3}-[[:digit:]]{1,2}-[[:digit:]]{1,2}$')
           And Not
                Regexp_Like(p_Colvalue,
                            '^[1-9][[:digit:]]{3}-((00|0)-((00|0)|[[:digit:]]{1,2})|[[:digit:]]{1,2}-(00|0))$')
           And Regexp_Substr(p_Colvalue, '[[:digit:]]{1,2}', 6) <= 12
           And Regexp_Substr(p_Colvalue, '[[:digit:]]{1,2}$') <= 31;
        Return 1;
      Exception
        When Others Then
          Return 0;
      End;
    End If;
  End If;
End f_Isdate;
/*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                get_ledger_id
  *   DESCRIPTION:
  *                取ledger_id 
  *
  *   HISTORY:
  *     1.00   2017-12-15   sun.zheng   Creation 
  *                          
  *
  * ==============================================*/ 
  --取公司ledger_id
Procedure get_ledger_id (P_ledger_id In Number) 
  Is 
Begin
          Update Cux_Gl_Detail_All cgda Set cgda.ledger_id=(
          Select Gls.Ledger_Id
            From Fnd_Flex_Value_Sets      Ffvs,
                 Fnd_Flex_Values_Vl       Ffv,
                 Gl_Ledger_Segment_Values Gls,
                 gl_ledgers gl
           Where Ffv.Flex_Value_Set_Id = Ffvs.Flex_Value_Set_Id
             And Ffvs.Flex_Value_Set_Name = '10_COA_CO'
             And Ffv.End_Date_Active Is Null
             And gl.LEDGER_ID=gls.LEDGER_ID
             And gl.LEDGER_CATEGORY_CODE='PRIMARY'
             And Ffv.Flex_Value = Gls.Segment_Value
             And Ffv.Summary_Flag = 'N'
                And gls.END_DATE Is Null
             And instr(gl.NAME,'停用')='0' 
             And ffv.DESCRIPTION=cgda.company_name
             )
          Where cgda.feedback1='PROCESS'
          And cgda.batch_id=const_batch_id
          And  cgda.ledger_id Is Null;
     Exception When Others Then 
       cux_uninterface_pkg.log(const_ledger,-9999,'procedure get_ledger_id 更新ledger_id 出现了错误'||Sqlerrm,const_batch_id); 
        
End;

/*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                get_bankflow_source_type
  *   DESCRIPTION:
  *                取银行流水bussiness_name
  *
  *   HISTORY:
  *     1.00   2017-12-15   sun.zheng   Creation 
  *                          
  *
  * ==============================================*/ 
  --update_bussiness_name
  Function get_bankflow_source_type (p_detial_id In Varchar2) Return Varchar2
    Is
    l_return Varchar2(2000);
    L_char5 Varchar2(500); --
    L_char3 Varchar2(500); --
    L_CURRENCY Varchar2(500);
    L_rCURRENCY Varchar2(500); 
    L_char6 Varchar2(500); --
    L_rChar5 Varchar2(500); --
    L_rChar3 Varchar2(500); --
    L_rChar6 Varchar2(500); --
    
    /* ----------------------要素使用说明-------------------
      条件1 COL_VARCHAR3  判断方向  贷 1 0 并
      条件2 COL_VARCHAR6    往来款 1 0 或
      条件2 COL_VARCHAR6    货款  2 0 或
      条件2 COL_VARCHAR6    出金  3 0 或
      条件2 COL_VARCHAR6    企业内部银行划转  4 0 并
      条件3 COL_VARCHAR1  是否为关联方  10_COA_IC 1 0                             
    */
    Begin
       --1.0 取要素的值
       Select cgda.col_varchar3,
              cgda.currency_code,
              cgda.col_varchar5,
              cgda.col_varchar6
        Into L_char3,L_CURRENCY,L_char5,L_char6
        From cux_gl_detail_all cgda Where cgda.detial_id=p_detial_id;
       --2.0 根据要素的值判断要素的返回结果
         --2.1 第一要素 是否为关联方 如果是：1，否0
                Select Count(1)
                  Into L_rChar5
                  From Fnd_Flex_Value_Sets Ffvs, Fnd_Flex_Values_Vl Ffv
                 Where Ffvs.Flex_Value_Set_Id = Ffv.Flex_Value_Set_Id
                   And Ffvs.Flex_Value_Set_Name = '10_COA_IC'
                   And Ffv.Enabled_Flag = 'Y'
                   And Ffv.Summary_Flag = 'N'
                   And Ffv.Description = L_char5;
          --2.2 第二要素 如果是贷返回1，否返回0
                If L_char3 ='贷' Then
                  L_rChar3:='1'; 
                Else 
                  L_rChar3:='0'; 
                End If ;
           --2.3 第三要素         
                If L_char6 ='出/入金' Then
                  L_rChar6:='001'; 
                Elsif L_char6 ='企业内部银行划转（本行为付方）' Then 
                  L_rChar6:='002'; 
                Elsif L_char6 ='企业内部银行划转（本行为收方）' Then 
                  L_rChar6:='003';  
                Elsif L_char6 ='收到采购退款-红字' Then 
                  L_rChar6:='004'; 
                Elsif L_char6 ='收到销售款项' Then 
                  L_rChar6:='005';
                Elsif L_char6 ='支付采购款项' Then 
                  L_rChar6:='006'; 
                Elsif L_char6 ='支付销售退款-红字' Then 
                  L_rChar6:='007'; 
                Elsif L_char6 ='支付银行手续费' Then 
                  L_rChar6:='008';
                Elsif L_char6 ='往来款' Then 
                  L_rChar6:='009'; 
                Elsif L_char6 ='物产进出口专用-RSD往来款' Then 
                  L_rChar6:='010'; 
                Elsif L_char6 ='物产进出口专用-金财汇顶往来款' Then 
                  L_rChar6:='011'; 
                Elsif L_char6 ='物产进出口专用-收到杭州银行理财包收益' Then 
                  L_rChar6:='012'; 
                Elsif L_char6 ='物产进出口专用-赎回杭州银行金钱包理财' Then 
                  L_rChar6:='013'; 
                Elsif L_char6 ='物产进出口专用-购买杭州银行金钱包理财' Then 
                  L_rChar6:='014'; 
                Elsif L_char6 ='支付营业费用（其他应付款中转）' Then 
                  L_rChar6:='015'; 
                Elsif L_char6 ='收到银行存款利息收入' Then 
                  L_rChar6:='016';                           
                Else
                  L_rChar6:='017';   
                End If ;
            --2.4 判断币种
               If L_CURRENCY = 'USD' Then 
               L_rCURRENCY:=2;
               Else
                 L_rCURRENCY:=1; 
               End If;
          l_return:=L_rChar3||L_rCURRENCY||L_rChar5||L_rChar6;
          
         
          Return l_return;
      Exception When Others Then 
        cux_uninterface_pkg.log(const_ledger,-9999,'get_bankflow_source_type 出现错误'||Sqlerrm,const_batch_id);
         Return -999; 
    End;
  

/*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                get_sale_source_type
  *   DESCRIPTION:
  *                取销售bussiness_name
  *
  *   HISTORY:
  *     1.00   2017-12-15   sun.zheng   Creation 
  *                          
  *
  * ==============================================*/ 
  --update_bussiness_name
  Function get_sale_source_type (p_detial_id In Varchar2) Return Varchar2
    Is
    l_return Varchar2(2000);
    L_char5 Varchar2(500); --
    L_CURRENCY Varchar2(500);
    L_rCURRENCY Varchar2(500); 
    L_char6 Varchar2(500); --
    L_rChar5 Varchar2(500); --
    L_rChar6 Varchar2(500);
    Begin
       --1.0 取要素的值
       Select 
              cgda.col_varchar1,
              cgda.currency_code
        Into L_char5,L_CURRENCY
        From cux_gl_detail_all cgda Where cgda.detial_id=p_detial_id;
       --2.0 根据要素的值判断要素的返回结果
         --2.1 第一要素 是否为关联方 如果是：1，否0
                Select Count(1)
                  Into L_rChar5
                  From Fnd_Flex_Value_Sets Ffvs, Fnd_Flex_Values_Vl Ffv
                 Where Ffvs.Flex_Value_Set_Id = Ffv.Flex_Value_Set_Id
                   And Ffvs.Flex_Value_Set_Name = '10_COA_IC'
                   And Ffv.Enabled_Flag = 'Y'
                   And Ffv.Summary_Flag = 'N'
                   And Ffv.Description = L_char5;
            --2.4 判断币种
               If L_CURRENCY = 'USD' Then 
               L_rCURRENCY:=2;
               Else
                 L_rCURRENCY:=1; 
               End If;
          l_return:=L_rChar5||L_rCURRENCY;
          Return l_return;
      Exception When Others Then 
        cux_uninterface_pkg.log(const_ledger,-9999,'get_sale_source_type 出现错误'||Sqlerrm,const_batch_id);
         Return -999; 
    End;
  
/*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                get_po_source_type
  *   DESCRIPTION:
  *                取采购bussiness_name
  *
  *   HISTORY:
  *     1.00   2017-12-15   sun.zheng   Creation 
  *                          
  *
  * ==============================================*/ 
  --update_bussiness_name
  Function get_po_source_type (p_detial_id In Varchar2) Return Varchar2
    Is
    l_return Varchar2(2000);
    L_char5 Varchar2(500); --
    L_CURRENCY Varchar2(500);
    L_rCURRENCY Varchar2(500); 
    L_char6 Varchar2(500); --
    L_rChar5 Varchar2(500); --
    L_rChar6 Varchar2(500);
    Begin
       --1.0 取要素的值
       Select 
              cgda.col_varchar1,
              cgda.currency_code
        Into L_char5,L_CURRENCY
        From cux_gl_detail_all cgda Where cgda.detial_id=p_detial_id;
       --2.0 根据要素的值判断要素的返回结果
         --2.1 第一要素 是否为关联方 如果是：1，否0
                Select Count(1)
                  Into L_rChar5
                  From Fnd_Flex_Value_Sets Ffvs, Fnd_Flex_Values_Vl Ffv
                 Where Ffvs.Flex_Value_Set_Id = Ffv.Flex_Value_Set_Id
                   And Ffvs.Flex_Value_Set_Name = '10_COA_IC'
                   And Ffv.Enabled_Flag = 'Y'
                   And Ffv.Summary_Flag = 'N'
                   And Ffv.Description = L_char5;
            --2.4 判断币种
               If L_CURRENCY = 'USD' Then 
               L_rCURRENCY:=2;
               Else
                 L_rCURRENCY:=1; 
               End If;
          l_return:=L_rChar5||L_rCURRENCY;
          Return l_return;
      Exception When Others Then 
        cux_uninterface_pkg.log(const_ledger,-9999,'get_po_source_type 出现错误'||Sqlerrm,const_batch_id);
         Return -999; 
    End;
  


/*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                update_bussiness_name
  *   DESCRIPTION:
  *                取bussiness_name
  *
  *   HISTORY:
  *     1.00   2017-12-15   sun.zheng   Creation 
  *                          
  *
  * ==============================================*/ 
  --update_bussiness_name
Procedure Update_bussiness_name (P_ledger_id In Varchar2) Is 
  l_base_column Varchar2(100);
  l_condition_value Varchar2(100);
  l_condition_id    Varchar2(100);
  L_base_VALUE           Varchar2(100);
  l_base_result          Varchar2(100);
  L_result_name               Varchar2(100);
  L_result_code               Varchar2(100);
  l_sql                  Varchar2(2000);
  cv Sys_Refcursor;
  Cursor get_detial_value Is 
     Select *  
     From Cux_Gl_Detail_All Cgda
     Where
     1=1  
     And cgda.ledger_id=P_ledger_id 
     And cgda.batch_id=const_batch_id
     And cgda.feedback1='PROCESS'; 
   Begin
     --取出值
     For get_detial In get_detial_value Loop
           --1.判断来源
           l_base_result:='XXX';
         Begin
           If get_detial.source_name = '银行流水导入' Then   ----------银行
              --1.1 值
              l_base_result:=get_bankflow_source_type(get_detial.detial_id);
              --1.2 condition id
                      
                   Select cgim.condit_id Into l_condition_id
                     From Cux_Gl_Intab_Map_Codition  Cgim,
                          Cux_Gl_Inter_Table_Mapping Cgtm
                    Where 1 = 1
                      And Cgim.Mapp_Id = Cgtm.Mapp_Id
                      And Cgtm.Attribute1 = '银行流水导入'
                      And Rownum = 1 ;  
                      
           Elsif  get_detial.source_name = '销售导入' Then   ----------销售导入
                   
            Select cgim.condition_value,cgim.outer_column_name,cgim.condit_id
            Into l_condition_value,l_base_column,l_condition_id
                     From Cux_Gl_Intab_Map_Codition  Cgim,
                          Cux_Gl_Inter_Table_Mapping Cgtm
                    Where 1 = 1
                      And Cgim.Mapp_Id = Cgtm.Mapp_Id
                      And Cgtm.Attribute1 = '销售导入'
                      And Rownum = 1 ;        
             --1.1 值
              l_base_result:=get_sale_source_type(get_detial.detial_id);
              
       Elsif  get_detial.source_name = '采购导入' Then   ----------采购导入
                   
            Select cgim.condition_value,cgim.outer_column_name,cgim.condit_id
            Into l_condition_value,l_base_column,l_condition_id
                     From Cux_Gl_Intab_Map_Codition  Cgim,
                          Cux_Gl_Inter_Table_Mapping Cgtm
                    Where 1 = 1
                      And Cgim.Mapp_Id = Cgtm.Mapp_Id
                      And Cgtm.Attribute1 = '采购导入'
                      And Rownum = 1 ;        
             --1.1 值
              l_base_result:=get_po_source_type(get_detial.detial_id); 
      
       Else ---通用型
       Null; 
       End If; 
       Exception When Others Then 
           cux_uninterface_pkg.log(const_ledger,-9999,'动态更新业务类型出错了get_detial'||get_detial.detial_id,const_batch_id); 
       End;
       --取对应的业务类型的名字         
      L_result_name:=Null;
      L_result_code:=Null; 
      
       Begin
         Select  Cgic.Bussiness_Name,cgic.bussiness_code Into L_result_name,L_result_code
          From Cux_Gl_Intmap_Con_Detial Cgic
         Where Cgic.Condit_Id = l_condition_id
         --And Cgic.Conresult_Value=l_base_result;
         And regexp_instr(l_base_result,'^'||replace(Cgic.Conresult_Value,'N','[[:alnum:]]'))>0;
       Exception When Others Then 
          cux_uninterface_pkg.log(const_ledger,get_detial.detial_id,'动态更新业务类型出错了'||Sqlerrm||'l_condition_id:'||l_condition_id||'l_base_result'||l_base_result,const_batch_id); 
       End;  
     Update Cux_Gl_Detail_All cgda Set cgda.bussiness_name=L_result_name, cgda.bussiness_code=L_result_code where cgda.detial_id=get_detial.detial_id;        
     End Loop;
   --  Commit;
  Exception When Others Then 
    cux_uninterface_pkg.log(const_ledger,-9999,'动态更新业务类型出错了'||Sqlerrm||'l_condition_id:'||l_condition_id||'l_base_result'||l_base_result,const_batch_id); 
End; 

 /*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                vaildate_ccid
  *   DESCRIPTION:
  *                验证科目组合ID
  *
  *   HISTORY:
  *     1.00   2017-12-15   sun.zheng   Creation 
  *                          
  *
  * ==============================================*/ 
  --验证科目组合
  Procedure vaildate_ccid(p_ledger_id In Number)
    Is
  l_Msg_Data         Varchar2(2000):='';
  l_Char_Of_Acc_Id Number := 50390;
  l_Ccid           Number := 1;
  Cursor Cur_Get Is
    Select * From Cux_Gl_Detail_Inter cgi Where 
    cgi.ledger_id=p_ledger_id 
    And cgi.attribute1='DRAFT'
    And cgi.batch_id=const_batch_id;
Begin
  --取CHART_OF_ACCOUNTS_IDID
  Select gl.CHART_OF_ACCOUNTS_ID Into l_Char_Of_Acc_Id From gl_ledgers gl Where gl.LEDGER_ID=p_ledger_id;
  For Rec_Date In Cur_Get Loop
    l_Ccid := 0;
    --插入GCC表
    If Fnd_Flex_Keyval.Validate_Segs(Operation        => 'CREATE_COMBINATION',
                                     Appl_Short_Name  => 'SQLGL',
                                     Key_Flex_Code    => 'GL#',
                                     Structure_Number => l_Char_Of_Acc_Id,
                                     Concat_Segments  => Rec_Date.Code_Combination_Segment) Then
      --返回CCID
      l_Ccid := Fnd_Flex_Ext.Get_Ccid(Application_Short_Name => 'SQLGL',
                                      Key_Flex_Code          => 'GL#',
                                      Structure_Number       => l_Char_Of_Acc_Id,
                                      Validation_Date        => To_Char(Sysdate,
                                                                        'YYYY/MM/DD'),
                                      Concatenated_Segments  => Rec_Date.Code_Combination_Segment);
     
    End If;
  
    If l_Ccid > 0 Then
      --插入成功:1 更新
      Update Cux_Gl_Detail_Inter Cgi
         Set Cgi.Code_Combination_Id = l_Ccid Where
                                       Cgi.Detaile_Id = Rec_Date.Detaile_Id And
                                       Cgi.Code_Combination_Segment =
                                       Rec_Date.Code_Combination_Segment
                                       And cgi.attribute1='DRAFT'
                                       ;
    Else
      l_Msg_Data := Fnd_Flex_Keyval.Error_Message;
      ------------失败：更新RESULT
      cux_uninterface_pkg.log(const_ledger,Rec_Date.Detaile_Id,'计算该行CCID出现错误：组合为'||Rec_Date.Code_Combination_Segment||'存在在错误：'||l_Msg_Data,const_batch_id); 
      
      /*Update Cux_Gl_Detail_Inter Cgi
         Set Cgi.Attribute1 = l_Msg_Data
       Where Cgi.Detaile_Id = Rec_Date.Detaile_Id
         And Cgi.Code_Combination_Segment =
             Rec_Date.Code_Combination_Segment;*/
    
    End If;
  
  End Loop;
  Exception When Others Then 
        cux_uninterface_pkg.log(const_ledger,-9999,'VAILDATE_CCID 出现错误'||Sqlerrm,const_batch_id);
End;
/*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                INSERT_GROUP_TABLE
  *   DESCRIPTION:
  *                插入汇总表
  *
  *   HISTORY:
  *     1.00   2017-12-15   sun.zheng   Creation 
  *                          
  *
  * ==============================================*/ 
  --插入汇总表
Procedure INSERT_GROUP_TABLE(p_ledger_id In Number)
  Is
  l_Iface_Rec             Gl_Interface%Rowtype;
 l_group_id              Number;
 L_journal_group_id      Number;
 L_seq                 Number;
 --CUX_GL_Journal_GROUP_S
Cursor cur_batch Is 
Select cgdi.batch_name,
       cgdi.batch_des,
       cgdi.batch_id,
       Min(cgdi.accounting_date) period_date,
       min(cgdi.ledger_id) ledger_id,
       min(cgdi.currency_code) current_code,
       Min(cgdi.source_name)   Source_name,
       Min(cgdi.category_name) catergory_name
  From Cux_Gl_Detail_Inter Cgdi
 Where Cgdi.Ledger_Id = p_ledger_id
  And cgdi.attribute1='DRAFT'
  And cgdi.batch_id=const_batch_id
 Group By Cgdi.Batch_Name, Cgdi.Batch_Des,cgdi.batch_id;
--header
Cursor cur_header(p_batch In Varchar2) Is 
Select Cgdi.Header_Name, 
       Cgdi.Header_Desc, 
       Sum(nvl(cgdi.header_reference,0)) refference
  From Cux_Gl_Detail_Inter Cgdi
 Where 1 = 1
   And cgdi.ledger_id=p_ledger_id
   And Cgdi.Batch_Name = p_batch
   And cgdi.attribute1='DRAFT'
   And cgdi.batch_id=const_batch_id
 Group By Cgdi.Header_Name, Cgdi.Header_Desc;
--line
Cursor cur_line(p_batch_name In Varchar2,p_header_name In Varchar2,p_header_dec_name In Varchar2) Is 
    Select Cgd.Batch_Name,
           Cgd.Header_Name,
           Cgd.Line_Drcr,
           Cgd.Line_Type_Name,
           Cgd.Code_Combination_Id,
           cgd.code_combination_segment,
           Cgd.Group_Rule,
           Min(Cgd.Line_Order) Line_Order,
           Sum(Nvl(Cgd.Entered_Dr, 0)) Enter_Dr,
           Sum(Nvl(Cgd.Entered_Cr, 0)) Enter_Cr,
           Sum(Nvl(cgd.stat_amount,0)) stat_amount,
          min(cgd.line_attribute1) line_attribute1,
          min(cgd.line_attribute2) line_attribute2,
          min(cgd.line_attribute3) line_attribute3,
          min(cgd.line_attribute4) line_attribute4,
          min(cgd.line_attribute5) line_attribute5,
          Min(cgd.line_description) line_description
      From Cux_Gl_Detail_Inter Cgd
     Where Cgd.Batch_Name = p_batch_name
       And Cgd.Header_Name = p_header_name
       And cgd.ledger_id=p_ledger_id
       And cgd.header_desc=p_header_dec_name
       And cgd.batch_id=const_batch_id
       And cgd.attribute1='DRAFT'
     Group By Cgd.Batch_Name,
              Cgd.Header_Name,
              Cgd.Code_Combination_Id,
              cgd.code_combination_segment，
              Cgd.Group_Rule,
              Cgd.Line_Drcr,
              Cgd.Line_Type_Name
     Order By Line_Order;
Begin
  --先删
  Delete Cux_Gl_Detail_Inter cgdi Where cgdi.ledger_id=p_ledger_id And cgdi.attribute1='FINAL' And cgdi.batch_id=const_batch_id;
  For get_batch In cur_batch Loop --循环批  
    Select Gl_Interface_Control_s.Nextval
    Into l_group_id
    From Dual;
    L_seq:=0;
    For get_header In cur_header(get_batch.batch_name) Loop --该批下循环多少个日记账
         Select CUX_GL_Journal_GROUP_S.Nextval 
         Into L_journal_group_id
         From dual;
         L_seq:=L_seq+1;
         For get_line In cur_line(get_batch.batch_name,get_header.Header_Name,get_header.header_desc) Loop
            Insert Into  Cux_Gl_Detail_Inter 
            ( LEDGER_ID
             ,accounting_date
             ,batch_name
             ,batch_des
             ,source_name
             ,category_name
             ,header_name
             ,header_desc
             ,header_reference
             ,CURRENCY_CODE
             ,LINE_ORDER
             ,line_type_name
             ,line_drcr
             ,code_combination_id
             ,code_combination_segment
             ,entered_dr
             ,entered_cr
             ,stat_amount
             ,attribute1
             ,GROUP_ID
             ,Group_Rule
             ,Journal_GROUP_ID
             ,line_attribute1
             ,line_attribute2
             ,line_attribute3
             ,line_attribute4
             ,line_attribute5
             ,line_description
             ,batch_id
            )
            Values
            ( get_batch.ledger_id
             ,get_batch.period_date
             ,get_batch.batch_name
             ,get_batch.batch_des
             ,get_batch.Source_name
             ,get_batch.catergory_name
             --,L_seq||get_header.Header_Name --日记账名字增加serquence
             ,get_header.Header_Name  
             ,get_header.Header_Desc
            -- ,get_header.refference
             ,L_journal_group_id
             ,get_batch.current_code
             ,get_line.Line_Order
             ,get_line.Line_Type_Name
             ,get_line.Line_Drcr
             ,get_line.Code_Combination_Id
             ,get_line.code_combination_segment
             ,get_line.Enter_Dr
             ,get_line.Enter_Cr
             ,get_line.stat_amount
             ,'FINAL'
             ,l_group_id
             ,get_line.Group_Rule
             ,L_journal_group_id
             ,get_line.line_attribute1
             ,get_line.line_attribute2
             ,get_line.line_attribute3
             ,get_line.line_attribute4
             ,get_line.line_attribute5
             ,get_line.line_description
             ,get_batch.batch_id
            );
         End Loop; 
    End Loop;
  End Loop;
  
  
  ---把组标识更新到行的明细上去 存在问题 多个公司的时候会有问题
Begin
  Update Cux_Gl_Detail_Inter Cgd
     Set Cgd.Group_Id =
         (Select Cgd2.Interface_Id
            From Cux_Gl_Detail_Inter Cgd2
           Where Cgd2.Attribute1 = 'FINAL'
             And Cgd.Batch_Name = Cgd2.Batch_Name
             And Cgd.Header_Name = Cgd2.Header_Name
             And cgd.header_desc =cgd2.header_desc
             And cgd.batch_id = const_batch_id
             And Cgd.Code_Combination_Id = Cgd2.Code_Combination_Id
             And Cgd.Code_Combination_Segment =
                 Cgd2.Code_Combination_Segment
             And Cgd.Line_Drcr = Cgd2.Line_Drcr
              And cgd2.batch_id=const_batch_id
             And Nvl(Cgd.Group_Rule, 1) = Nvl(Cgd2.Group_Rule, 1)
             And Cgd.Line_Type_Name = Cgd2.Line_Type_Name)
   Where Nvl(Cgd.Attribute1, 'DRAFT') = 'DRAFT'
   And cgd.batch_id=const_batch_id
   And cgd.ledger_id=p_ledger_id;
Exception
  When Others Then
    cux_uninterface_pkg.log(const_ledger,-9999,'汇总更新回明细失败-INSERT_GROUP_TABLE'||Sqlerrm,const_batch_id);  
  
  
End;
End;
  
 /*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                INSERT_GROUP_TABLE
  *   DESCRIPTION:
  *                更新弹性域信息
  *
  *   HISTORY:
  *     1.00   2017-12-15   sun.zheng   Creation 
  *                          
  *
  * ==============================================*/ 
  --更新弹性域信息
  
  
  Procedure update_attribute_line(p_ledger_id In Number)
    Is 
    Begin
      
    ----更新日记账行交易对象
    Update Cux_Gl_Detail_Inter cgdi Set cgdi.Attribute9=cgdi.line_attribute4 Where  cgdi.line_attribute4 Is Not Null And  cgdi.batch_id=const_batch_id;
    
    
     Update Cux_Gl_Detail_Inter Cgdi
      Set Cgdi.Line_Attribute4 =
          nvl((Select Ffv.Flex_Value
             From Fnd_Flex_Values_Vl Ffv, Fnd_Flex_Value_Sets Ffvs
            Where 1 = 1
              And Ffvs.Flex_Value_Set_Id = Ffv.Flex_Value_Set_Id
              And Ffvs.Flex_Value_Set_Name = 'CUX_GL_ALL_PARTY'
              And (Ffv.Description = Cgdi.Line_Attribute4 Or Ffv.FLEX_VALUE = Cgdi.Line_Attribute4)
              And Ffv.Attribute1 In
                  (Select t.Party_Gategory
                     From Cux_Assist_Account_Map t
                    Where t.Account_Num = Regexp_Substr(Cgdi.Code_Combination_Segment,
                                                        '[^\.]+',
                                                        1,
                                                        4))
              And rownum=1                                          
                                                        ),'error')
      Where Cgdi.Attribute1 = 'FINAL'
      And Cgdi.Line_Attribute4 Is Not Null
      And Cgdi.Ledger_Id = p_ledger_id
      And cgdi.batch_id=const_batch_id;
      ---更新有错误插日志
      For cur_get In (Select * From Cux_Gl_Detail_Inter cgdi Where  Cgdi.Attribute1 = 'FINAL'
                      And NVL(Cgdi.Line_Attribute4,'1')='error'    
                      And Cgdi.Ledger_Id = p_ledger_id
                      And cgdi.batch_id=const_batch_id)Loop
                cux_uninterface_pkg.log(const_ledger,cur_get.detaile_id,'更新行交易对象有问题,对象为'||cur_get.attribute9||'_科目为:'||cur_get.code_combination_segment,const_batch_id);             
      End Loop;
      
      Exception When Others Then 
         cux_uninterface_pkg.log(const_ledger,-9999,'更新行弹性域信息失败'||Sqlerrm,const_batch_id);    
    End;
  
  /*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                update_attribute_header
  *   DESCRIPTION:
  *                更新日记账头弹性域信息
  *
  *   HISTORY:
  *     1.00   2017-12-15   sun.zheng   Creation 
  *                          
  *
  * ==============================================*/ 
  --更新日记账头弹性域信息
  Procedure update_attribute_header(p_ledger_id In Number)
    Is 
     L_column  Varchar2(100):='';
     l_header  Number;
          Cursor get_je_header Is 
          Select Gjh.Je_Header_Id, cgda.bussiness_name,cgda.bussiness_code
            From Gl_Je_Headers Gjh, 
                 Cux_Gl_Detail_Inter Cgi, 
                 Cux_Gl_Detail_Inter Cgid,
                 CUX_GL_DETAIL_ALL   cgda
           Where to_char(Cgi.Journal_Group_Id) = Gjh.EXTERNAL_REFERENCE
             And Cgi.Interface_Id = Cgid.Group_Id
             And cgda.detial_id=cgid.detaile_id
             And Cgi.Attribute1 = 'FINAL'
             And cgi.ledger_id=gjh.LEDGER_ID
             And cgi.ledger_id=p_ledger_id
             Group By Gjh.Je_Header_Id, cgda.bussiness_name,cgda.bussiness_code;

          ---2.根据业务类型取汇总的列
          Cursor get_column_name(P_type_name In Varchar2) Is 
          Select Replace(Cabh.Column_Name, 'HEADER_', '') Column_Name
                 ,cabh.summaryed From 
                  Cux_Bussiness_Type_All Cbta,
                  Cux_Ass_Bat_Hea_All    Cabh
            Where Cbta.Bussiness_Code = P_type_name
              And Cabh.Assign_Type In('H') ---H 代表是头信息
              And Cabh.Ledger_Id = p_ledger_id
              And Cabh.Bussiness_Id = Cbta.Bussiness_Id
              And cabh.column_name Like '%ATTRIBUTE%';
          Begin
          ---如果是汇总就用SUM
           cux_conc_utl.log_msg('642');
          For cur_header In get_je_header Loop ---循环头
            cux_conc_utl.log_msg('644');
            For cur_type In get_column_name(cur_header.bussiness_code) Loop 
            L_column :=cur_type.Column_Name;
            l_header :=cur_header.Je_Header_Id;
                If cur_type.summaryed = 'Y' Then --如果是汇总用sum
                       Begin
                         dbms_output.put_line(648);
                        execute immediate 'Update Gl_Je_Headers Gjh2
                         Set Gjh2.'||L_column||' =
                             (Select SUM(Attri) Result
                                From (Select Min(Cgdid.Header_'||L_column||') Attri, Cgdid.Detaile_Id
                                        From Cux_Gl_Detail_Inter Cgdid,
                                             Cux_Gl_Detail_Inter Cgdih,
                                             Gl_Je_Headers       Gjh
                                       Where Gjh.External_Reference =
                                             To_Char(Cgdih.Journal_Group_Id)
                                         And Cgdih.Interface_Id = Cgdid.Group_Id
                                         And Gjh.Je_Header_Id = Gjh2.Je_Header_Id
                                         And cgdih.attribute1=''FINAL''
                                       Group By Cgdid.Detaile_Id))
                                       Where Gjh2.Je_Header_Id=:1'
                                       Using l_header;
                         Exception When Others Then 
                           dbms_output.put_line('更新日记账头弹性域出错了');
                            cux_uninterface_pkg.log(const_ledger,-9999,'更新日记账头弹性域出错了'||Sqlerrm,const_batch_id);   
                       end;
                  Else                             --如果不是汇总就取一个就可以了
                    Begin
                       cux_conc_utl.log_msg('L_column'||L_column);
                       cux_conc_utl.log_msg('l_header'||l_header);
                        execute immediate 'Update Gl_Je_Headers Gjh2
                         Set Gjh2.'||L_column||' =
                             (Select MIN(Attri) Result
                                From (Select Min(Cgdid.Header_'||L_column||') Attri, Cgdid.Detaile_Id
                                        From Cux_Gl_Detail_Inter Cgdid,
                                             Cux_Gl_Detail_Inter Cgdih,
                                             Gl_Je_Headers       Gjh
                                       Where Gjh.External_Reference =
                                             To_Char(Cgdih.Journal_Group_Id)
                                         And Cgdih.Interface_Id = Cgdid.Group_Id
                                         And Gjh.Je_Header_Id = Gjh2.Je_Header_Id
                                         And cgdih.attribute1=''FINAL''
                                       Group By Cgdid.Detaile_Id))
                                       Where Gjh2.Je_Header_Id=:1'
                                       Using l_header;
                          Exception When Others Then 
                           dbms_output.put_line('更新日记账头弹性域出错了');
                            cux_uninterface_pkg.log(const_ledger,-9999,'更新日记账头弹性域出错了'||Sqlerrm,const_batch_id);
                       end;
                  End If;
               End Loop;
           End Loop;
     End;
 
  /*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                update_attribute_header
  *   DESCRIPTION:
  *                更新状态信息
  *
  *   HISTORY:
  *     1.00   2017-12-15   sun.zheng   Creation 
  *                          
  *
  * ==============================================*/ 
  --更新状态信息
  Procedure update_status(p_ledger_id In Number)
    Is 
    Begin
--------------------------------------------------------成功的日记账---------------------------------
      --1.更新最后的日记账明细表
          Update Cux_Gl_Detail_Inter tab1 Set attribute1='SUCCESS'
          Where Exists
          ( 
          Select cgdi.attribute1,cgdi.interface_id
            From Gl_Je_Lines Gjl, Cux_Gl_Detail_Inter Cgdi
           Where Cgdi.Interface_Id = Gjl.REFERENCE_1
             And Cgdi.Attribute1 = 'FINAL'
             And cgdi.ledger_id=p_ledger_id
             And tab1.interface_id=cgdi.interface_id
          )
          And tab1.attribute1='FINAL'
          And tab1.ledger_id=p_ledger_id
          And tab1.batch_id = Const_Batch_Id
          ;

            --2.更新汇总之前的行明细表
              --  Update CUX_GL_DETAIL_INTER cgi Set cgi.attribute1='LINE_CREATE' Where cgi.ledger_id=p_ledger_id And cgi.attribute1='DRAFT';
                
          Update Cux_Gl_Detail_Inter tab1 Set tab1.attribute1='LINE_CREATE'
          Where 
          Exists
          (
          Select 1
            From Cux_Gl_Detail_Inter Cgdi, Cux_Gl_Detail_Inter Cgdi2
           Where Cgdi2.Interface_Id = Cgdi.Group_Id
             And cgdi.detaile_id=tab1.detaile_id
             And cgdi.ledger_id=p_ledger_id
             And Cgdi2.Attribute1 = 'SUCCESS'
             And cgdi.attribute1='DRAFT'
          )
          And tab1.attribute1='DRAFT'
          And tab1.batch_id = Const_Batch_Id
          ;

              --3.更新基础表
            
          Update Cux_Gl_Detail_All tab2 Set tab2.feedback1='CREATED',tab2.feedback2=Null
          Where Exists
          (
          Select 1
            From Cux_Gl_Detail_All Cgda, Cux_Gl_Detail_Inter Cgdi
           Where Cgda.Detial_Id = Cgdi.Detaile_Id
             And Cgdi.Attribute1 = 'LINE_CREATE'
             And cgda.detial_id=tab2.detial_id
             And cgda.ledger_id=p_ledger_id
             And Cgda.Feedback1 = 'PROCESS'
          )
          And tab2.feedback1='PROCESS'
          And tab2.batch_id = Const_Batch_Id
          ;
          
            --4更新基础表的批名和日记账名
             Update Cux_Gl_Detail_All Cgda
                    Set (Cgda.Batch_Name, Cgda.Header_Name,cgda.gl_batch_id) =
                        (Select Distinct Gjb.Name, Gjh.Name,gjb.JE_BATCH_ID
                           From Gl_Je_Batches       Gjb,
                                Gl_Je_Headers       Gjh,
                                Cux_Gl_Detail_Inter Cgd,
                                Cux_Gl_Detail_Inter Cgd2
                          Where Gjb.Je_Batch_Id = Gjh.Je_Batch_Id
                            And Cgd.Header_Reference = Gjh.External_Reference
                            And Cgd2.Group_Id = Cgd.Interface_Id
                            And Cgda.Detial_Id = Cgd2.Detaile_Id)
                  Where Exists (Select 1
                           From Gl_Je_Batches       Gjb,
                                Gl_Je_Headers       Gjh,
                                Cux_Gl_Detail_Inter Cgd,
                                Cux_Gl_Detail_Inter Cgd2
                          Where Gjb.Je_Batch_Id = Gjh.Je_Batch_Id
                            And Cgd.Header_Reference = Gjh.External_Reference
                            And Cgd2.Group_Id = Cgd.Interface_Id
                            And Cgda.Detial_Id = Cgd2.Detaile_Id)
                  And cgda.batch_id=Const_Batch_Id
                  And cgda.feedback1='CREATED'
                  And cgda.ledger_id= p_ledger_id
                 And cgda.batch_name Is Null;
------------------------------------失败更新----------------------------------------
            --5.更新基础表信息,如果feekback1还是new就代表着已经失败了
            Update Cux_Gl_Detail_All Tab2
               Set Tab2.Feedback2 = 'FAILED',tab2.feedback1='NEW'
             Where Tab2.Feedback1 = 'PROCESS'
               And tab2.ledger_id = p_ledger_id
               And Tab2.Batch_Id = Const_Batch_Id;
   Exception When Others Then 
     
 cux_uninterface_pkg.log(const_ledger,-9999,'更新最后状态出错',const_batch_id); 
       

  End;
 /*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                GET_BATCH_NAME
  *   DESCRIPTION:
  *                取批的名称
  *
  *   HISTORY:
  *     1.00   2017-12-15   sun.zheng   Creation 
  *                          ---取批的名称的定义只考虑了name,desc, source,catergry ,attribute 需要以后考虑
  *
  * ==============================================*/
 --取批的信息
Procedure GET_BATCH_INFO(p_detail_Id In Number)
  Is 
 Cursor get_infro Is  
 Select Cbta.Bussiness_Id,
        Cgda.Ledger_Id, 
        Cgda.Detial_Id,
        cabh.header_id,
        cabh.type_name,
        cabh.assign_type,
        cabh.column_name,
        cabh.je_source_name,
        cabh.user_je_category_name
   From Cux_Gl_Detail_All      Cgda,
        Cux_Bussiness_Type_All Cbta,
        Cux_Ass_Bat_Hea_All    Cabh
  Where Cgda.Bussiness_Code = Cbta.Bussiness_Code
   -- And Cbta.Bussiness_Code = 'FR0006'
    And Cabh.Assign_Type In('B','S') ---B 代表批，S代表来源类型
    And Cabh.Ledger_Id = Cgda.Ledger_Id
    And Cabh.Bussiness_Id = Cbta.Bussiness_Id
    And cgda.detial_id=p_detail_Id;
  Cursor get_group (hedader_ID In Varchar2)Is 
  Select Carg.Header_Id, 
         Carg.Group_Id, 
         Carg.Group_Name, 
         Carg.Cal_Level
  From Cux_Ass_Rule_Group_All Carg --规则组表
 Where Carg.Header_Id=hedader_ID   ---BATCH_NAME ID
 And carg.cal_level='Y';           --计算级别 此处应该是Y如果是N应该让get_rule_result处理
 
 l_batch      Varchar2(240);    

 L_source_name Varchar2(240);
 L_category_name Varchar2(240);
 Begin
   For cur_in In get_infro Loop---存在多行 BATCH_NAME BATCH_DESC
     l_batch:='';
    If cur_in.assign_type = 'B' Then 
     For cur_group In get_group(cur_in.header_id) Loop
         l_batch:= get_rule_result(cur_group.group_id,p_detail_Id);
     End Loop;   
         Begin
           If f_Isdate(l_batch,'yes') = 1 And cur_in.column_name='ACCOUNTING_DATE' Then
             execute immediate 'Update CUX_GL_DETAIL_INTER Set '||cur_in.column_name||' = to_date('''||l_batch||''',''YYYY-MM-DD'') Where  DETAILE_ID= :1'
             Using p_detail_Id;
            Elsif f_Isdate(l_batch,'yes') = 0 And cur_in.column_name='ACCOUNTING_DATE' Then 
              
             cux_uninterface_pkg.log(const_ledger,p_detail_Id,'计算该行的账期存在问题'||Sqlerrm||'值'||l_batch,const_batch_id); 
           Else
            execute immediate 'Update CUX_GL_DETAIL_INTER Set '||cur_in.column_name||' = '''||l_batch||''' Where  DETAILE_ID= :1'
             Using p_detail_Id;       
           End If ;
          Exception When Others Then
            cux_uninterface_pkg.log(const_ledger,p_detail_Id,'计算该行批定义的'||cur_in.column_name||'存在错误!'||Sqlerrm,const_batch_id); 
            dbms_output.put_line('更新批信息:'||Sqlerrm);  
         End;    
     Elsif cur_in.assign_type = 'S' Then ---日记账的来源和类型定义（应该不存在错误）
       L_source_name:=cur_in.je_source_name;
       L_category_name:=cur_in.user_je_category_name;
     End If; 
   End Loop;      
  Update CUX_GL_DETAIL_INTER cgda 
  Set  cgda.source_name=L_source_name
       ,cgda.category_name=L_category_name
  Where cgda.detaile_id=p_detail_Id;
 Exception When Others Then 
        cux_uninterface_pkg.log(const_ledger,-9999,'GET_BATCH_INFO 出现错误'||Sqlerrm,const_batch_id);
     
 End;
 
/*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                GET_HEADER_INFO
  *   DESCRIPTION:
  *                定义日记账头信息
  *
  *   HISTORY:
  *     1.00   2017-12-15   sun.zheng   Creation 
  *                          
  *
  * ==============================================*/ 
  --取日记账头信息
Procedure GET_HEADER_INFO(p_detail_Id In Number)
  Is 
 
 Cursor get_infro Is  
 Select Cbta.Bussiness_Id,
        Cgda.Ledger_Id, 
        Cgda.Detial_Id,
        cabh.header_id,
        cabh.type_name,
        cabh.assign_type,
        cabh.column_name,
        cabh.je_source_name,
        cabh.user_je_category_name
   From Cux_Gl_Detail_All      Cgda,
        Cux_Bussiness_Type_All Cbta,
        Cux_Ass_Bat_Hea_All    Cabh
  Where Cgda.Bussiness_Code = Cbta.Bussiness_Code
    And Cabh.Assign_Type In('H') ---H 代表是头信息
    And Cabh.Ledger_Id = Cgda.Ledger_Id
    And Cabh.Bussiness_Id = Cbta.Bussiness_Id
    And cgda.detial_id=p_detail_Id;
  Cursor get_group (hedader_ID In Varchar2)Is 
  Select Carg.Header_Id, 
         Carg.Group_Id, 
         Carg.Group_Name, 
         Carg.Cal_Level
  From Cux_Ass_Rule_Group_All Carg    --规则组表
 Where Carg.Header_Id=hedader_ID   
 And carg.cal_level='Y';           ----计算级别 此处应该是Y如果是N应该让get_rule_result处理
 
 l_result      Varchar2(240);    
 Begin
   For cur_in In get_infro Loop---对头那些字段进行了定义
      
    l_result:='';
 
     For cur_group In get_group(cur_in.header_id) Loop
         l_result:= get_rule_result(cur_group.group_id,p_detail_Id);
     End Loop;
     Begin
         ---动态更新已经指定的列
        execute immediate 'Update CUX_GL_DETAIL_INTER Set '||cur_in.column_name||' = '''||l_result||''' Where  DETAILE_ID= :1'
        Using p_detail_Id;
     Exception When Others Then 
            cux_uninterface_pkg.log(const_ledger,p_detail_Id,'计算该行日记账头信息的列：'||cur_in.column_name||'存在在错误'||Sqlerrm||'l_result'||l_result,const_batch_id);              
     End;
   End Loop;
        
   Exception When Others Then 
        cux_uninterface_pkg.log(const_ledger,-9999,'GET_HEADER_INFO 出现错误'||Sqlerrm,const_batch_id);
  
 End; 
/*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                GET_LINE_INFO
  *   DESCRIPTION:
  *                定义日记行信息
  *
  *   HISTORY:
  *     1.00   2017-12-15   sun.zheng   Creation 
  *                          
  *
  * ==============================================*/ 
  --取日记行信息
Procedure GET_LINE_INFO(p_detail_Id In Number)
  Is 
---1.先取会产生多少日记行
l_result Varchar2(240):='';
L_COA_SEGMENT Varchar2(2000):='';
l_mapping Varchar2(2000):='';
l_values  Varchar2(2000):='';
L_count   Number:=0;
L_column_value Varchar2(2000):='';
l_column  Varchar2(200):='';
l_sql     Varchar2(2000):='';
l_group   Varchar2(200):='';
l_line_group Varchar2(2000):='';--行分组规则
cv Sys_Refcursor;  ---动态游标
Cursor cur_infro Is 
Select Cbta.Bussiness_Id,
        Cgda.Ledger_Id, 
        Cgda.Detial_Id,
        cabh.header_id,
        cabh.type_name,
        cabh.assign_type,
        cabh.order_type,
        cabh.line_ypte_name,
        cabh.line_ypte_code,
        cabh.summaryed,
        cabh.debt,
        cgda.batch_id
   From Cux_Gl_Detail_All      Cgda,
        Cux_Bussiness_Type_All Cbta,
        Cux_Ass_Bat_Hea_All    Cabh
  Where Cgda.Bussiness_Code = Cbta.Bussiness_Code
    And Cabh.Assign_Type In('L') ---L 代表行信息
    And Cabh.Ledger_Id = Cgda.Ledger_Id
    And Cabh.Bussiness_Id = Cbta.Bussiness_Id
    And cgda.detial_id=p_detail_Id
    ;
--2.取改当钱日记账行定义的那些列需要定义规则
Cursor cur_line(p_header_id In Varchar2) Is 
Select Calt.Line_Column, Calt.Line_Desc,calt.line_type_id
  From Cux_Ass_Lines_Type_All Calt
 Where 1 = 1
   And Nvl(Calt.Souce_Type, 'N') = 'L' --目前只取非科目的柜子
   And Calt.Header_Id = p_header_id;
--3.取当前列定义组的规则
 Cursor get_group (p_line_ID In Varchar2)Is 
  Select Carg.Header_Id, 
         Carg.Group_Id, 
         Carg.Group_Name, 
         Carg.Cal_Level,
         carg.attribute1
  From Cux_Ass_Rule_Group_All Carg    --规则组表
 Where Carg.Line_Type_Id=p_line_ID   
 And carg.cal_level='Y'           ----计算级别 此处应该是Y如果是N应该让get_rule_result处理
 ;
--4.取科目定义
Cursor get_coa(p_header_id In Varchar2) Is
Select Calt.Segment_Code,
       Calt.Segment_Desc,
       Calt.Data_Value,
       Calt.Mapp,
       Calt.Line_Type_Id,
       calt.souce_type,
       calt.attribute1,
       calt.out_mapping_id,
       calt.out_mapping_name,
       calt.BASE_COLUMN,
       calt.attribute2
  From Cux_Ass_Lines_Type_All Calt
 Where 1 = 1
   And Nvl(Calt.Souce_Type, 'N') <> 'L' --目前只取非科目的柜子
   And Calt.Header_Id = p_header_id
 Order By To_Number(Nvl(Calt.Line_Column, 1)), Calt.Line_Type_Id;
Begin
  

  --0.清除数据
Delete CUX_GL_DETAIL_INTER  cgdi Where cgdi.detaile_id=p_detail_Id And cgdi.attribute1='DRAFT' And cgdi.batch_id=const_batch_id;
 
 For infor In cur_infro Loop --看有多少行
   --0.标记是否插入了
   L_count:=L_count+1;
   --1.插入类型
   Insert Into CUX_GL_DETAIL_INTER(
   LEDGER_ID,
   DETAILE_ID,
   LINE_ORDER,
   LINE_TYPE_NAME,
   LINE_TYPE_CODE,
   LINE_SUMMARYED,
   LINE_DRCR,
   attribute1,
   batch_id
   )Values(
   infor.Ledger_Id
   ,infor.Detial_Id
   ,infor.order_type
   ,infor.line_ypte_name
   ,infor.line_ypte_code
   ,infor.summaryed
   ,infor.debt
   ,'DRAFT'
   ,infor.batch_id
   );
   --SQL%ROWCOUNT。
   
     For line In cur_line(infor.header_id) Loop--每个行都定义了那些的规则
      --取具体行的每个列的规则
      l_result:='';
      For cur_group In get_group(line.line_type_id) Loop
         l_result:= CUX_INTERFACE_RULE_PKG.get_rule_result(cur_group.group_id,p_detail_Id);
      End Loop;
     ---2.动态更新已经指定的列
     Begin  
       
      execute immediate 'Update CUX_GL_DETAIL_INTER Set '
                        ||line.Line_Column||' = '''||l_result||
                        ''' Where  DETAILE_ID= :1 and LINE_TYPE_NAME = :2'
      Using p_detail_Id,infor.line_ypte_name;
     Exception When Others Then 
       cux_uninterface_pkg.log(const_ledger,p_detail_Id,'日记账行信息更新更新'||line.Line_Column||'用数据'||l_result||'存在错误'||Sqlerrm,const_batch_id);
       dbms_output.put_line('存在错误更新'||line.Line_Column||'用数据'||l_result); 
     End;
    --------------------------------------------3科目逻辑-------------------------
     L_COA_SEGMENT:='';
     L_column_value:='';
     l_column:='';
     For cur_coa In get_coa(infor.header_id) Loop  
       If cur_coa.SOUCE_TYPE='INPUT' Then                  --3.1固定值 
          L_COA_SEGMENT:=L_COA_SEGMENT||cur_coa.Data_Value||'.';
       Elsif cur_coa.SOUCE_TYPE='BASETABLE' Then           --3.2 根据值取该科目对应的值集
           --取指定列的值：比如指定了col_valuer2
            Begin
              L_column_value:='';
              l_column      :='';
              l_sql:= 'Select '|| Replace(cur_coa.BASE_COLUMN,'''','''''')||' From Cux_Gl_Detail_All cgda Where cgda.detial_id='||p_detail_Id;
                     Open cv For l_sql;
                     Loop
                     Fetch cv Into  L_column_value;
                     Exit When cv%Notfound;
                     End Loop;
                    Close cv;
                
             --该值对应的值集的值
                   Select Ffv.Flex_Value_Meaning Into l_column
                    From Fnd_Flex_Values_Vl Ffv
                     Where Ffv.Flex_Value_Set_Id = cur_coa.attribute1
                      And ffv.ENABLED_FLAG='Y'
                      And Ffv.Description=L_column_value
                      And ffv.SUMMARY_FLAG='N';
     
                    
              L_COA_SEGMENT:=L_COA_SEGMENT||l_column||'.';
            Exception When Others Then 
               cux_uninterface_pkg.log(const_ledger,p_detail_Id,'计算科目值用基表存在问题'||Sqlerrm||'L_column_value:'||L_column_value||'cur_coa.attribute1:'||cur_coa.attribute1,const_batch_id);
            End;
       Elsif cur_coa.SOUCE_TYPE='MAPPING' Then --3.3 存在外部映射关系 
         l_column:=cur_coa.BASE_COLUMN;
         l_mapping:='未取值';
            Begin  
             --3.2 动态取值
               l_sql:= 'Select '||cur_coa.BASE_COLUMN||' From Cux_Gl_Detail_All cgda Where cgda.detial_id='||p_detail_Id;
                l_values:='';
                If l_column Is Null Then
                   l_mapping:='未定义映射基表！';                
               Else
                   Open cv For l_sql;
                   Loop
                   Fetch cv Into  l_values;
                   Exit When cv%Notfound;
                   End Loop;
                  Close cv;
                   --确定最后映射关系值
                   Begin                                        
                      Select 
                          decode(NVL(cur_coa.attribute2,'D'),'C',
                                 Cmla.Sub_Value,
                                 Cmla.Main_Value
                                 ) mappvalue Into l_mapping
                        From Cux_Mapp_Lines_All Cmla
                       Where Cmla.Header_Id = cur_coa.out_mapping_id
                         And Rtrim(rtrim(Cmla.Input_Value ),Chr(9)) =  rtrim(rtrim(l_values,' '),chr(9));
                   Exception When Others Then 
                     cux_uninterface_pkg.log(const_ledger,p_detail_Id,'此数据:'||l_values||'无法找到科目映射',const_batch_id); 
                   End;
                 End If;    
             Exception When Others Then
                l_mapping:='映射取值有错误';
                cux_uninterface_pkg.log(const_ledger,p_detail_Id,'计算该行科目映射的时候存在在错误'||Sqlerrm,const_batch_id); 
                dbms_output.put_line(Sqlerrm);
             End;      
             L_COA_SEGMENT:=L_COA_SEGMENT||l_mapping||'.';   
       Else                                                                                 --3.4 存在内部映射映射
         
         l_mapping:='找不到取值逻辑';
         
         L_COA_SEGMENT:=L_COA_SEGMENT||l_mapping||'.';
       End if;
     End Loop;
     ---end 3 把科目组合更新到数据上去
     
     Begin
      L_COA_SEGMENT:=Rtrim(L_COA_SEGMENT,'.'); --去除最右侧的.
      Update  CUX_GL_DETAIL_INTER cgdi Set  cgdi.code_combination_segment=L_COA_SEGMENT 
      Where cgdi.detaile_id=p_detail_Id
        and LINE_TYPE_NAME=infor.line_ypte_name
      ; 
     Exception When Others Then 
       cux_uninterface_pkg.log(const_ledger,p_detail_Id,'计算该行科目组合的时候存在在错误'||Sqlerrm,const_batch_id); 
       dbms_output.put_line('更新科目失败');
     End;
    End Loop;
    ---4.确定该行是否需要分组
    If infor.summaryed=upper('S') Then
      l_line_group:='';
      For cur_in In (Select Car.Data_Column
                            From Cux_Ass_Rule_Detail_All Car
                           Where Car.Description = 'GROUP_RULE'
                             And Car.Header_Id = infor.header_id
                           Order By Car.Sequences) Loop
          --4.1 取分组列的值                
          l_sql:= 'Select '||cur_in.Data_Column||' From Cux_Gl_Detail_All cgda Where cgda.detial_id='||p_detail_Id;   
                   Open cv For l_sql;
                   Loop
                   Fetch cv Into  l_group;
                   Exit When cv%Notfound;
                   End Loop;
                  Close cv;
         
         l_line_group:=l_line_group||l_group||'-'; --拼接汇总规则               
      End Loop;
      l_line_group:=Rtrim(l_line_group,'-');
      Begin
      Update CUX_GL_DETAIL_INTER cgdi Set cgdi.group_rule=l_line_group
       Where cgdi.detaile_id=p_detail_Id
        and LINE_TYPE_NAME=infor.line_ypte_name;
      Exception When Others Then 
        cux_uninterface_pkg.log(const_ledger,p_detail_Id,'计算该行汇总规则存在错误'||Sqlerrm,const_batch_id); 
       dbms_output.put_line('更新汇总规则失败,detial:='||p_detail_Id);
      End;
     Else
        Update CUX_GL_DETAIL_INTER cgdi Set cgdi.group_rule=p_detail_Id||infor.line_ypte_name
       Where cgdi.detaile_id=p_detail_Id
        and LINE_TYPE_NAME=infor.line_ypte_name;
      -------------------------------4end------------------------------
    End If;
 End Loop;
 
 --如果不存在信息
 If L_count=0 Then 
   cux_uninterface_pkg.log(const_ledger,p_detail_Id,'插入日记账行信息出错,没有取到定义的规则信息',const_batch_id);
 End If;
 Exception When Others Then 
    cux_uninterface_pkg.log(const_ledger,p_detail_Id,'插入日记账行信息出错'||Sqlerrm,const_batch_id); 
End;
/*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                create_gl_record
  *   DESCRIPTION:
  *                插入日记账接口表
  *
  *   HISTORY:
  *     1.00   2017-12-15   sun.zheng   Creation 
  *                          
  *
  * ==============================================*/ 
  --插入日记账接口表
Function create_gl_record(p_ledger_id In Number) Return Varchar2 Is 
    l_Iface_Rec             Gl_Interface%Rowtype;
  l_Request_Id            Number;
  l_Set_Of_Books_Id       Number := p_ledger_id;
  l_User_Je_Source_Name   Gl_Je_Sources_Tl.User_Je_Source_Name%Type;
  l_User_Je_Category_Name Gl_Je_Categories_Tl.User_Je_Category_Name%Type;
  l_Je_Source_Name        Gl_Je_Sources_Tl.Je_Source_Name%Type;
  l_Interface_Run_Id      Number;
  L_ENTERED_DR Number;
  L_ENTERED_CR Number;
  
  l_Phase                 Varchar2(80);
  l_Status                Varchar2(80);
  l_Dev_Phase             Varchar2(80);
  l_Dev_Status            Varchar2(80);
  l_Message               Varchar2(2000);
  l_Wait_Status           Boolean;
Begin

  For cur_in  In (  Select *
                      From Cux_Gl_Detail_Inter Cgdi
                     Where Cgdi.Attribute1 = 'FINAL'
                       And cgdi.batch_id=const_batch_id
                       And (nvl(Cgdi.Entered_Dr,0)+nvl(cgdi.entered_cr,0))<>0
                       And Cgdi.Ledger_Id = p_ledger_id
                    ) Loop               
    l_Iface_Rec.Status                := 'NEW';
    l_Iface_Rec.Set_Of_Books_Id       := cur_in.LEDGER_ID;
    l_Iface_Rec.ledger_id             :=cur_in.LEDGER_ID;
    l_Iface_Rec.Accounting_Date       := cur_in.ACCOUNTING_DATE;
    l_Iface_Rec.Date_Created          := Sysdate;
    l_Iface_Rec.Created_By            := Fnd_Global.User_Id;
    l_Iface_Rec.Actual_Flag           := 'A';
    l_Iface_Rec.User_Je_Source_Name   := cur_in.SOURCE_NAME;
    l_Iface_Rec.User_Je_Category_Name := cur_in.CATEGORY_NAME;
    l_Iface_Rec.Reference1            := regexp_replace(cur_in.BATCH_NAME,'@+[[:alnum:]]+@','');
    l_Iface_Rec.Reference2            := regexp_replace(cur_in.BATCH_DES,'@+[[:alnum:]]+@','');
    l_Iface_Rec.Reference4            := regexp_replace(cur_in.HEADER_NAME,'@+[[:alnum:]]+@','');
    l_Iface_Rec.Reference5            := regexp_replace(cur_in.HEADER_DESC,'@+[[:alnum:]]+@','');
    l_Iface_Rec.Reference6            := cur_in.HEADER_REFERENCE; --头参考说明
    l_Iface_Rec.Reference10           := regexp_replace(cur_in.LINE_DESCRIPTION,'@+[[:alnum:]]+@',''); --行说明
    l_Iface_Rec.Currency_Code         := nvl(cur_in.CURRENCY_CODE,'CNY');
    l_Iface_Rec.Reference21           := cur_in.interface_id;---行对应
    l_iface_Rec.PERIOD_NAME           :='2018-02';
    --dr
    l_Iface_Rec.Code_Combination_Id := cur_in.CODE_COMBINATION_ID;
    L_ENTERED_DR:=cur_in.ENTERED_DR; --如果借方是0 
    If L_ENTERED_DR = 0 Then
        l_Iface_Rec.Entered_Dr          := Null;
        l_Iface_Rec.Accounted_Dr        := Null;
        l_Iface_Rec.Entered_Cr          := cur_in.ENTERED_CR;
        l_Iface_Rec.Accounted_Cr        := Null;
        
    Else
        l_Iface_Rec.Entered_Dr          := cur_in.ENTERED_DR;
        l_Iface_Rec.Accounted_Dr        := Null;
        l_Iface_Rec.Entered_Cr          := Null;
        l_Iface_Rec.Accounted_Cr        := Null;
    End If;
        l_Iface_Rec.attribute1              :=cur_in.line_attribute1;
        l_Iface_Rec.attribute2              :=cur_in.line_attribute2;
        l_Iface_Rec.attribute3              :=cur_in.line_attribute3;
        l_Iface_Rec.attribute4              :=cur_in.line_attribute4;
        l_Iface_Rec.attribute5              :=cur_in.line_attribute5;
        
    If cur_in.stat_amount <>0 And cur_in.stat_amount Is Not Null Then 
    l_Iface_Rec.Stat_Amount         := cur_in.Stat_Amount;
    Else
     l_Iface_Rec.Stat_Amount         := Null; 
    End If;
    l_Iface_Rec.Group_Id            :=cur_in.GROUP_ID;
    
  Insert Into Gl_Interface Values l_Iface_Rec;
 
  
  End Loop;
  --submit request
  
  ---同一批次
    Select Gl_Interface_Control_s.Nextval Into l_Interface_Run_Id From Dual;
  For cur_get In (
                  Select Cgdi.Group_Id, Cgdi.Source_Name
                    From Cux_Gl_Detail_Inter Cgdi
                   Where Cgdi.Attribute1 = 'FINAL'
                     And Cgdi.Ledger_Id = p_ledger_id
                     And cgdi.batch_id = const_batch_id
                   Group By Cgdi.Group_Id, Cgdi.Source_Name
                  ) Loop 

  Select Max(Jes.Je_Source_Name)
    Into l_Je_Source_Name
    From Gl_Je_Sources_Vl Jes
   Where
   Jes.User_Je_Source_Name = cur_get.Source_Name;
  Insert Into Gl_Interface_Control
    (Je_Source_Name, Group_Id, Interface_Run_Id, Set_Of_Books_Id, Status)
  Values
    (l_Je_Source_Name,
     cur_get.Group_Id,
     l_Interface_Run_Id,
     p_ledger_id,
     'S');
  End Loop;
  l_Request_Id := Fnd_Request.Submit_Request('SQLGL',
                                             'GLLEZL',
                                             '',
                                             To_Char(Sysdate,
                                                     'YYYY/MM/DD HH24:MI:SS'),
                                             False,
                                             l_Interface_Run_Id,
                                             l_Set_Of_Books_Id,
                                             'N',
                                             Null,
                                             Null,
                                             'N',
                                             'O',
                                             Chr(0));

  Commit;
  Dbms_Output.Put_Line('Request ID ' || l_Request_Id);
  If l_Request_Id > 0 Then
    --请求等待
    l_Wait_Status := Fnd_Concurrent.Wait_For_Request(Request_Id => l_Request_Id,
                                                     Interval   => 10,
                                                     Max_Wait   => 0,
                                                     Phase      => l_Phase,
                                                     Status     => l_Status,
                                                     Dev_Phase  => l_Dev_Phase,
                                                     Dev_Status => l_Dev_Status,
                                                     Message    => l_Message);
    Dbms_Output.Put_Line('l_status:' || l_Status);
    Return l_Status;
   Else
       Return 'NO';
  End If;
  Exception When Others Then 
    cux_uninterface_pkg.log(const_ledger,-9999,'日记账导入接口失败'||sqlerrm,const_batch_id);
    Return 'NO';
End;
  
/*================================================
  * ===============================================
  *   PROGRAM NAME:
  *                create_gl_record
  *   DESCRIPTION:
  *                插入日记账接口表
  *
  *   HISTORY:
  *     1.00   2017-12-15   sun.zheng   Creation 
  *                          
  *
  * ==============================================*/ 
  --插入日记账接口表
Procedure out_put_log(p_ledger In Number)Is 
 l_column Varchar2(2000);
 l_sql    Varchar2(2000);
 l_mapping   Varchar2(4000);
 l_count     Number;
 L_i         Number;
 l_detial    Number;
 cv Sys_Refcursor;  ---动态游标
  Begin
    cux_conc_utl.out_msg('<html><head>
    <meta http-equiv="Content-Language" content="zh-cn">
    <meta http-equiv="Content-Type" content="text/html;">
    <title>日记账导入失败日志</title>
    <STYLE>
     BODY  {  background-color : #FFFFFF;  font-family : Verdana;  font-size : 10pt;color : Black;}
     TR,TD{  font-family : Verdana;  font-size : 10pt;  color : Black;}
   H1{
    text-shadow: 4px 3px 0px #fff, 9px 8px 0px rgba(0,0,0,0.15);
     }

    </STYLE>
    </head><body><p>
    <H1 align=center>日记账导入失败日志</h1>');
    cux_conc_utl.out_msg('
    <table width="100%" border="1" cellspacing="0" cellpadding="2"
    style="BORDER-COLLAPSE: collapse" bordercolorlight="#000000" bordercolordark="#000000">');
    cux_conc_utl.out_msg('<tr bgcolor="#BBBBBB">');
   ---输出表题头
   cux_conc_utl.log_msg(10);
   l_count:=0;
   
 For cur_type In (Select Cgd.Source_Name
                             From Cux_Gl_Detail_All Cgd
                            Where Cgd.Feedback1 = 'NEW'
                              And Cgd.Ledger_Id = p_ledger
                              Group By Cgd.Source_Name )Loop
                   
   For cur_in In(Select  Cgitt.Type_Id,
                         Cgit.Outer_Table_Name,
                         Cgitt.System_Name,
                         Cgitt.Type_Name,
                         Cgitt.Base_Table,
                         cgit.ora_table_colname
                    From Cux_Gl_Inter_Table_Detail Cgit, Cux_Gl_Inter_Table_Type Cgitt
                   Where Cgit.Type_Id = Cgitt.Type_Id
                   And Cgitt.base_table =cur_type.source_name
                   --And cgit.outer_table_name=cur_type.source_name
                   ) Loop
  
       cux_conc_utl.out_msg('<td nowrap>'||cur_in.Outer_Table_Name||'</td>');    
       l_column:=l_column||cur_in.ora_table_colname||'||''@''||';
       l_count:=l_count+1;
    End Loop; 

      cux_conc_utl.out_msg('</tr>');
      l_column:=rtrim(l_column,'||');
    ---输出你导入的明细信息
      l_sql:='Select detial_id, '||l_column||'  
              From Cux_Gl_Detail_All Cgda
              Where Exists(Select * From cux_neal_log cnl Where cnl.line=cgda.detial_id
              )
              and cgda.Source_Name='''||cur_type.source_name||'''
              And cgda.ledger_id='||p_ledger;
        cux_conc_utl.log_msg(l_sql);
    
       Open cv For l_sql;
                   Loop
                   ---输出明细
                   cux_conc_utl.out_msg('<tr bgcolor="#7fffd4">');  
                   Fetch cv Into l_detial ,l_mapping;
                   L_i:=1;
                     cux_conc_utl.log_msg('l_count'||l_count);
                   For L_i In 1..l_count Loop
                     --cux_conc_utl.log_msg(12.6);
                     cux_conc_utl.out_msg('<td nowrap>'||regexp_substr(l_mapping,'[^\@]+',1,L_i) ||'</td>');
               
                   End Loop;
                
                   cux_conc_utl.out_msg('<tr>');
                   ---输出错误信息
           
                    For cur_in In (Select cnl.line,
                                cnl.messages
                                From  
                                cux_neal_log cnl 
                                Where 
                                cnl.pack_name=p_ledger
                                And cnl.line=l_detial 
                                And Exists (
                                    Select 1
                                    From Cux_Gl_Detail_All Cgda
                                   Where Cgda.Ledger_Id = p_ledger
                                     And cgda.detial_id=cnl.line
                                     And Cgda.Feedback1 = 'NEW'
                                 )Order By cnl.line,cnl.seq) Loop 
                          cux_conc_utl.out_msg('<tr><td nowrap colspan="'||l_count||'">'||cur_in.messages||'</td></tr>');                 
                    End Loop;
                   Exit When cv%Notfound;
                   End Loop;
        Close cv;
End Loop;
  End; 

Procedure main( ERRBUF      OUT VARCHAR2,
                RETCODE     OUT VARCHAR2,
                p_ledger In Number,
                p_detial In Number,
                P_batch    In Varchar2 Default 2,
                p_user     In Varchar2) 
  Is PRAGMA AUTONOMOUS_TRANSACTION;
  l_status Varchar2(40);
  l_count Number;
  L_MSG_DATA      VARCHAR2(2000);
  l_Resp Number;
  l_Co   Varchar2(100);
  L_errbuf     Varchar2(500);
  L_retcode    Varchar2(500);
  L_user       Varchar2(200);
  ---取明细表所有的数据
 Cursor get_detail(p_ledger_id In Varchar2) Is 
   Select *
  From Cux_Gl_Detail_All Cgda
 Where Cgda.Ledger_Id = p_ledger_id
   And cgda.detial_id=nvl(p_detial,cgda.detial_id)
   And nvl(cgda.batch_id,2)=nvl(P_batch,2)
   And Cgda.Feedback1 = 'PROCESS';---状态代表最新过来的。
  Begin
  --1.设置每个行的批的名称和描述
     --.1.1 删除所有的日志新
    const_batch_id:=P_batch;
   Begin
     Update Cux_Gl_Detail_All cga Set cga.feedback1='PROCESS' Where cga.feedback1='NEW' And cga.batch_id=P_batch;
     Commit;
   End;
   --Delete CUX_UNINTERFACE_LOG cnl; ---最后要删了()
   Delete CUX_UNINTERFACE_LOG cnl Where cnl.batch_id=const_batch_id;
   Commit;
  cux_uninterface_pkg.log(const_ledger,-7777,'1523',const_batch_id);
    dbms_output.put_line(1);
    get_ledger_id(100);
  --取登陆用户 
   Begin 
   Select fu.USER_ID Into L_user From fnd_user fu Where fu.USER_NAME=upper(p_user);
   Exception When Others Then 
     L_user:=1111;---指定童华
     cux_uninterface_pkg.log(const_ledger,-7777,'取用户出错 参数 p_user'||p_user,const_batch_id); 
  End;   
  --可能多个公司套循环然后增加预制文件的输入 
  For Curget In (Select Cgda.Ledger_Id,cgda.company_name
                   From Cux_Gl_Detail_All Cgda
                  Where 1 = 1
                    And Cgda.Feedback1 = 'PROCESS'
                    And nvl(cgda.batch_id,2)=nvl(P_batch,2)
                  Group By Cgda.Ledger_Id,cgda.company_name) Loop
   const_ledger:=Curget.Ledger_Id;  --设置全局的ledger_id               
       cux_uninterface_pkg.log(const_ledger,-7777,'1534', const_batch_id);             
     --取公司的前编码
    Begin
    Select Ffv.FLEX_VALUE   Into l_Co
      From Fnd_Flex_Value_Sets Ffvs, Fnd_Flex_Values_Vl Ffv
     Where Ffv.Flex_Value_Set_Id = Ffvs.Flex_Value_Set_Id
       And Ffvs.FLEX_VALUE_SET_NAME='10_COA_CO'
       And ffv.END_DATE_ACTIVE Is Null
       And ffv.DESCRIPTION=curget.company_name
       And ffv.SUMMARY_FLAG = 'N';
    cux_uninterface_pkg.log(const_ledger,-7777,'1540',const_batch_id);  
    --对应公司的GL超级用户的职责
    Select Fr.Responsibility_Id
      Into l_Resp
      From Fnd_Responsibility_Vl Fr
     Where Fr.Responsibility_Name Like l_Co||'%GL%总账超级用户';
     --预制文件
  
      cux_uninterface_pkg.log(const_ledger,-7777,'1547',const_batch_id);  
      
     Fnd_Global.Apps_Initialize(User_Id      => L_user,
                                Resp_Id      => l_Resp,
                                Resp_Appl_Id => 101);
       Exception When Others Then 
          cux_uninterface_pkg.log(const_ledger,-9999,'日记账取配置文件出错'||sqlerrm,const_batch_id);
    End; 
    
  --  Delete cux_neal_log cnl Where cnl.pack_name =to_char(Curget.Ledger_Id); --删除日志文件
     cux_uninterface_pkg.log(const_ledger,-7777,'1551',const_batch_id);
     --1.2 取ledger_id
  
     --1.1 更新业务类型
   cux_uninterface_pkg.log(const_ledger,-7777,'1552',const_batch_id);
    Update_bussiness_name (Curget.Ledger_Id);
     cux_uninterface_pkg.log(const_ledger,-7777,'1557',const_batch_id);
     --1.3 
  For cur_detail In get_detail(Curget.Ledger_Id) Loop
    --cux_uninterface_pkg.log(const_ledger,-7777,'15575');
      GET_LINE_INFO(cur_detail.detial_id);
      GET_BATCH_INFO(cur_detail.detial_id);
      GET_HEADER_INFO(cur_detail.detial_id);
  End loop; 
  --更新科目
     cux_uninterface_pkg.log(const_ledger,-7777,'1581',const_batch_id);
   
  vaildate_ccid(Curget.Ledger_Id);
    cux_uninterface_pkg.log(const_ledger,-7777,'1568',const_batch_id);
  ---不存在错误信息再进行如下的操作
  
   cux_uninterface_pkg.log(const_ledger,-7777,'1585',const_batch_id);
    --进行汇总
   INSERT_GROUP_TABLE(Curget.Ledger_Id);
  --更新行弹性域信息
   cux_uninterface_pkg.log(const_ledger,-7777,'1589',const_batch_id);
   update_attribute_line(Curget.Ledger_Id);
 l_count:=0;
 Select Count(1) Into l_count
                        From  
                        CUX_UNINTERFACE_LOG cnl 
                        Where
                        cnl.batch_id=const_batch_id
                        And NVL(cnl.detial_id,1) <> -7777  ---排除日志的
                        ;
                         
  If l_count = 0 Then ---不存基本的错误信息
  --插入接口表生成数据
   cux_uninterface_pkg.log(const_ledger,-7777,'1592',const_batch_id);
    l_Status:=create_gl_record(Curget.Ledger_Id);
  --更新日记账头弹性域必须在生成日记账之后才能更新
   cux_uninterface_pkg.log(const_ledger,-7777,'1595',const_batch_id);
   update_attribute_header(Curget.Ledger_Id);
   cux_uninterface_pkg.log(const_ledger,-7777,'1597',const_batch_id);
   If l_Status Not In('正常','Normal') Then 
    cux_uninterface_pkg.log(const_ledger,-9999,'日记账导入失败',const_batch_id);    
   End If;
  End If;
   --更新所有的状态信息final--->success,NEW-created
    update_status(Curget.Ledger_Id);  ---更新状态 可能存在部分进入和不是全部进入所以都需要更新一下      
      --更新原始单据编号
   Begin
    L_errbuf:=Null;
    CUX_DOC_NUM_AUTO_PKG.main(L_errbuf,L_retcode,Curget.Ledger_Id);
    If L_errbuf Is Not Null Then
       cux_uninterface_pkg.log(const_ledger,-9999,'原始单据编号错误'||L_errbuf,const_batch_id);  
    End If;
   End;
    
  End Loop;
   --更新现金流量表
    L_errbuf:=Null;
   Begin
   CUX_CF_UPDATE_ATTR_INTER(L_errbuf,L_retcode,const_batch_id);
       If L_errbuf Is Not Null Then 
         cux_uninterface_pkg.log(const_ledger,-9999,'更新现金流量错误'||L_errbuf,const_batch_id); 
       End If;
   End;
  Commit;---提交
  Exception WHEN FND_API.G_EXC_ERROR THEN
      CUX_CONC_UTL.LOG_MESSAGE_LIST;
      RETCODE := '1';
      ERRBUF  := L_MSG_DATA;
    WHEN FND_API.G_EXC_UNEXPECTED_ERROR THEN
      CUX_CONC_UTL.LOG_MESSAGE_LIST;
      RETCODE := '2';
      ERRBUF  := L_MSG_DATA;
    WHEN OTHERS THEN
      FND_MSG_PUB.ADD_EXC_MSG(P_PKG_NAME       => G_PKG_NAME,
                              P_PROCEDURE_NAME => 'POST_CHECK');
      CUX_CONC_UTL.LOG_MESSAGE_LIST;
      RETCODE := '2';
      ERRBUF  := SQLERRM;
      
  End;  
end CUX_INTERFACE_RULE_PKG;
/
