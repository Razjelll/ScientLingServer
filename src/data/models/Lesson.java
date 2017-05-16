package data.models;

/**
 * Created by Razjelll on 29.04.2017.
 */
public class Lesson {
    private long mId;
    private String mName;
    private int mNumber;
    private long mSetId;

    public long getId(){return mId;}
    public void setId(long id){mId = id;}

    public String getName(){return mName;}
    public void setName(String name){mName = name;}

    public int getNumber(){return mNumber;}
    public void setNumber(int number){mNumber = number;}

    public long getSetId(){return mSetId;}
    public void setSetId(long setid){mSetId = setid;}

    @Override
    public String toString(){
        return new StringBuilder("id: ").append(mId).append("\n")
                .append("name: ").append(mName).append("\n")
                .append("number: ").append(mNumber).append("\n")
                .append("setId: ").append(mSetId).toString();
    }
}
