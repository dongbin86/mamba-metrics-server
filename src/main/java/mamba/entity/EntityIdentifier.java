package mamba.entity;

/**
 * @author dongbin  @Date: 10/28/16
 */
public class EntityIdentifier implements Comparable<EntityIdentifier> {


    private String id;
    private String type;

    public EntityIdentifier(String id, String type) {
        this.id = id;
        this.type = type;
    }


    public String getId() {
        return id;
    }


    public String getType() {
        return type;
    }

    @Override
    public int compareTo(EntityIdentifier other) {
        int c = type.compareTo(other.type);
        if (c != 0) return c;
        return id.compareTo(other.id);
    }

    @Override
    public int hashCode() {
        // generated by eclipse
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        // generated by eclipse
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EntityIdentifier other = (EntityIdentifier) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (type == null) {
            if (other.type != null)
                return false;
        } else if (!type.equals(other.type))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "{ id: " + id + ", type: " + type + " }";
    }
}
