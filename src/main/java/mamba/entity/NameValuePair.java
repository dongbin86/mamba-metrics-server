package mamba.entity;

/**
 * @author dongbin  @Date: 10/28/16
 */
public class NameValuePair {


    String name;
    Object value;

    public NameValuePair(String name, Object value) {
        this.name = name;
        this.value = value;
    }


    public String getName() {

        return name;
    }


    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "{ name: " + name + ", value: " + value + " }";
    }
}
