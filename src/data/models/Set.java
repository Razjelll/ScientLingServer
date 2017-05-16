package data.models;

public class Set {
    private long mId;
    private String mName;
    private long mL1;
    private long mL2;
    private long mAuthorId;
    private String mDescription;

    public long getId(){return mId;}
    public void setId(long id){mId = id;}

    public String getName(){return mName;}
    public void setName(String name){mName = name;}

    public long getL1(){return mL1;}
    public void setL1(long l1){mL1 = l1;}

    public long getL2(){return mL2;}
    public void setL2(long l2){mL2 = l2;}

    public long getAuthorId(){return mAuthorId;}
    public void setAuthorId(long id){mAuthorId = id;}

    public String getDescription(){return mDescription;}
    public void setDescription(String description){mDescription = description;}

    @Override
    public String toString(){
        return new StringBuilder("id: ").append(mId).append("\n")
                .append("name: ").append(mName).append("\n")
                .append("l1: ").append(mL1).append("\n")
                .append("l2: ").append(mL2).append("\n")
                .append("author: ").append(mAuthorId).append("\n")
                .append("description: ").append(mDescription).toString();
    }
}
