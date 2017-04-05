<?php
namespace ElasticPhpSimple;

class Model{
    CONST _AND = "_and";
    CONST _OR = "_or";
    protected $_doc;
    protected $_prev_index;
    protected $_conditions=[];
    protected $_remove_keys=[];
    protected $_builder;
    protected $_order=[];

    public function __construct(){
        $this->_builder = new Builder();
    }
    public function getBuilder(){
        return $this->_builder;
    }
    public function esIndex(){
        return "index";
    }
    public function esType(){
        return "type";
    }

    protected function _filedIndex($field){
        return '_index_'.$field;
    }

    public function select($field){
        return $this->_builder->select($field);
    }

    public function term($field ,$value){
        return $this->condition($field ,$value ,__FUNCTION__);
    }
    public function notTerm($field ,$value){
        return $this->condition($field ,$value ,__FUNCTION__);
    }
    public function match($field ,$value){
        return $this->condition($field ,$value ,__FUNCTION__);
    }
    public function notMatch($field ,$value){
        return $this->condition($field ,$value ,__FUNCTION__);
    }
    public function in($field ,$value){
        return $this->condition($field ,$value ,__FUNCTION__);
    }
    public function notIn($field ,$value){
        return $this->condition($field ,$value ,__FUNCTION__);
    }
    public function like($field ,$value){
        return $this->condition($field ,$value ,__FUNCTION__);
    }
    public function notLike($field ,$value){
        return $this->condition($field ,$value ,__FUNCTION__);
    }
    public function range($field ,$value){
        return $this->condition($field ,$value ,__FUNCTION__);
    }
    public function missing($field){
        return $this->condition($field ,"" ,__FUNCTION__);
    }
    public function notMissing($field){
        return $this->condition($field ,"" ,__FUNCTION__);
    }

    protected function getValidFieldProperName($field){
        $i=0;
        $properName = $field.$i;
        while (isset($this->$properName)){
            $i++;
            $properName = $field.$i;
        }
        return $properName;
    }

    //对象属性赋值
    public function condition($field ,$value ,$func){
        $propertyName = $this->getValidFieldProperName($field);
        $this->$propertyName = [[$field ,$value ,$func]];
        $index = $this->_filedIndex($propertyName);
        //push

        $this->$index = array_push($this->_conditions ,'$'.$propertyName); //索引数
        //index
        $this->_prev_index = --$this->$index;       //链表
        return $this;
    }
    public function withInner($field ,$type="and", $fieldIndex=0){
        return $this->with($field ,$type ,'in', $fieldIndex);
    }
    public function withOuter($field ,$type='and', $fieldIndex=0){
        return $this->with($field ,$type ,'out', $fieldIndex=0);
    }
    //建立字段关联
    protected function with($field ,$type='and' ,$loc='in' ,$fieldIndex='0'){
        $type = '_'.strtolower($type);
        if(in_array($type , [self::_AND ,self::_OR])){
            $prevField  = $this->_conditions[$this->_prev_index];
            $prevIndex = $this->_filedIndex($prevField);
            $propertyName= $field.$fieldIndex;  //默认第一个
            $withIndex = $this->_filedIndex($propertyName);

            if(!isset($this->$propertyName)){
                //TODO
                return $this;
            }
            $playload = $this->$propertyName;
            if($loc == 'in'){
                if(!isset($playload[$type])){
                    $playload = [$type=>$playload];
                }
                array_push($playload[$type] ,$prevField);
            }elseif($loc == 'out'){
                if(isset($playload[self::_AND]) || isset($playload[self::_OR])) {
                    $playload = [$playload];
                }
                $playload = [$type=>$playload];
                array_push($playload[$type] ,$prevField);
            }
            $this->_remove_keys[] = $this->_prev_index;
            $this->$propertyName = $playload;
        }
        return $this;
    }
    public function groupByOrder($field ,$order){
        return $this->_builder->groupByOrder($field ,$order);
    }
    public function groupBy($field ,$in=null ,$flag=true){
        return $this->_builder->groupBy($field ,$in ,$flag);
    }
    public function dateHistogram($field ,$in=null ,$flag=true){
        return $this->_builder->dateHistogram($field ,$in ,$flag);
    }
    public function histogram($field ,$in=null ,$flag=true){
        return $this->_builder->histogram($field ,$in ,$flag);
    }
    public function orderBy($orderBy ,$order){
        return $this->_builder->orderBy($orderBy ,$order) ;
    }
    public function offset($from=0,$size=10){
        return $this->_builder->offset($from ,$size) ;
    }
    public function min($field ,$as=null ,$flag=true){
        return $this->_builder->min($field ,$as ,$flag);
    }
    public function max($field ,$as=null ,$flag=true){
        return $this->_builder->max($field ,$as ,$flag);
    }
    public function sum($field ,$as=null ,$flag=true){
        return $this->_builder->sum($field ,$as ,$flag);
    }
    public function avg($field ,$as=null ,$flag=true){
        return $this->_builder->avg($field ,$as ,$flag);
    }
    public function count($field ,$as=null ,$flag=true){
        return $this->_builder->count($field ,$as ,$flag);
    }
    public function distinctCount($field ,$as=null ,$flag=true){
        return $this->_builder->cardinalCount($field ,$as ,$flag);
    }
    public function rangeAgg($field, $as=null, $flag=true){
        return $this->_builder->rangeAgg($field, $as, $flag);
    }
    public function dsl(){
        //process conditions
        if(!empty($this->_conditions)){
            //get root field
            if(!empty($this->_remove_keys)){
                foreach($this->_remove_keys as $_index){
                    if(isset($this->_conditions[$_index])){
                        unset($this->_conditions[$_index]);
                    }
                }
            }
            $firstField = current($this->_conditions);
            $ret = $this->parse($firstField);
            //last build
            $field = str_replace('$','',$firstField);
            $data = $this->$field;
            if(!isset($data[self::_AND]) && !isset($data[self::_OR])){
                $this->_builder->_and($ret);
            }
        }
        $this->_doc = [
            Type::EINDEX => $this->esIndex(),
            Type::ETYPE => $this->esType(),
            Type::EBODY => $this->_builder->build()
        ];
        return $this->_doc;
    }

    //核心解析函数
    protected function parse($data){
        $conditions =[] ;
        $_and_condition=[];
        $_or_condition=[];
        //1、变量处理获取的数据
        if(is_string($data) && 0 === strpos($data ,'$')){
            //获取变量数据
            $data = str_replace('$','',$data);
            $data = $this->$data;   //找到key对应数据
            if(!isset($data[self::_AND]) && !isset($data[self::_OR])){
                $data = current($data);
            }
        }
        //2、数组处理获取的数据
        //只有and 或者or条件才会有多条
        if(isset($data[self::_AND])){//形如:['_and'=>[0=>[0=>McompanyId,1=>263,2=>match],1=>$Department,2=>$__]]
            foreach($data[self::_AND] as $row){ //取出字段名
                $_and_condition[] = $this->parse($row); //递归获取值
            }
            $conditions = call_user_func_array(array($this->_builder ,self::_AND),$_and_condition);
        }elseif(isset($data[self::_OR])){//形如:['_or'=>[0=>[0=>__,1=>,2=>missing],1=>$Activated,2=>$StandardFlag]]
            foreach($data[self::_OR] as $row){
                $_or_condition[] = $this->parse($row);
            }
            $conditions = call_user_func_array(array($this->_builder ,self::_OR),$_or_condition);
        }else{
            //只有一条 直接处理,形如:[0=>McompanyId,1=>263,2=>match]
            $conditions = $this->call($data);
        }
        return $conditions;
    }
    protected function call($data){
        if(isset($data[0]) && isset($data[1]) && isset($data[2]) && !is_array($data[0])){
            return $this->_builder->$data[2]($data[0] ,$data[1]);
        }else{
            return [];
        }
    }
    public function __set($name ,$value){
        $this->$name = $value;
    }
    public function __get($name){
        return isset($this->$name)?$this->$name:null;
    }
}
