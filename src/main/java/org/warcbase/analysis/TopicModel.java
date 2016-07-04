package org.warcbase.analysis;


import cc.mallet.util.*;
import cc.mallet.types.*;
import cc.mallet.pipe.*;
import cc.mallet.pipe.iterator.*;
import cc.mallet.topics.*;

import java.util.*;
import java.util.regex.*;
import java.io.*;

public class TopicModel {

    public static void main(String[] args) throws Exception {
        ArrayList<String> aa = new ArrayList<String>();
        aa.add("apple fruit orange banana strawberry computer television");
        aa.add("mango fruit avengers hulk thor");

        String[] ob1=new String[aa.size()];
        ob1=aa.toArray(ob1);
        ArrayList<Pipe> pipeList= new ArrayList<Pipe>();

        pipeList.add(new CharSequence2TokenSequence());
//....
        pipeList.add(new TokenSequence2FeatureSequence());

        InstanceList training=new InstanceList(new SerialPipes(pipeList));

        training.addThruPipe(new StringArrayIterator(ob1));

        int numTopics = 3;
        ParallelTopicModel model = new ParallelTopicModel(numTopics, 1.0, 0.01);

        model.addInstances(training);

        // Use two parallel samplers, which each look at one half the corpus and combine
        //  statistics after every iteration.
        //model.setNumThreads(2);

        // Run the model for 50 iterations and stop (this is for testing only,
        //  for real applications, use 1000 to 2000 iterations)
        model.setNumIterations(1000);
        model.estimate();

        Object[][] arr = model.getTopWords(10);
        for (int i =0; i < arr.length; i++) {
            System.out.println(" ****** " + i);
            for (int j=0; j < arr[i].length; j++) {
                System.out.println(" " + arr[i][j]);
            }
        }

    }

}

