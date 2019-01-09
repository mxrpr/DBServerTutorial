import com.mixer.util.JSONRep;

public final class Dog implements JSONRep {

    public String pname;
    public int age;
    public String owner;

    public Dog(){}

    public Dog(final String pname, final int age, final String owner){
        this.pname = pname;
        this.age = age;
        this.owner = owner;
    }

    @Override
    public String toJSON() {
        return String.format("{\"pname\" : \"%s\", \"age\": %d, \"owner\": \"%s\", }", this.pname, this.age, this.owner);
    }
}
