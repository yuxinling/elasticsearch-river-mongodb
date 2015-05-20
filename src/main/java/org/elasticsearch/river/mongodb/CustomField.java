package org.elasticsearch.river.mongodb;

/**
 * User: yuyangning
 * Date: 5/19/15
 * Time: 11:43 AM
 */
public class CustomField {
    private String name;
    private Object value;
    private String ref;
    private boolean lowcase = false;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }

    public boolean isLowcase() {
        return lowcase;
    }

    public void setLowcase(boolean lowcase) {
        this.lowcase = lowcase;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            CustomField field = (CustomField) o;
            return this.getName().equals(field.getName());
        } else {
            return false;
        }
    }

    public int hashCode() {
        return this.getName().hashCode();
    }

}
