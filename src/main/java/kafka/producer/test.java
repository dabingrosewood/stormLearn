package kafka.producer;

import java.util.HashMap;


public class test {
    public static void main(String args[]){
        HashMap map=new HashMap(1,20);
        map.put(2,"对应了3");
        map.put(22,"对应了2");
        System.out.println(map.get(22));


    }
}
