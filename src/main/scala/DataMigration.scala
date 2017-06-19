/*
 * Copyright (c) ${year} ${name} <${email}> All Rights Reserved.
 *
 * This software is the confidential and proprietary information of
 * Netuitive, Inc. and certain third parties ("Confidential Information").
 * You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement
 * you entered into with Netuitive.
 *
 * NETUITIVE MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY OF
 * THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE,
 * OR NON-INFRINGEMENT. NETUITIVE SHALL NOT BE LIABLE FOR ANY DAMAGES
 * SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING OR DISTRIBUTING
 * THIS SOFTWARE OR ITS DERIVATIVES.
 */

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.{TTLOption, WriteConf}
import org.apache.spark.sql.cassandra._


/**
  * Based entirely on: http://rustyrazorblade.com/2015/01/introduction-to-spark-cassandra/
  *
  * Created by bspindler on 6/17/17.
  */
case class FoodToUserIndex(food: String, user: String)

object DataMigration {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "192.168.99.100")
      .set("spark.cassandra.connection.port", "32774")

    val sc = new SparkContext("local", "test", conf)

    // class to represent cass row

    // make the associateion
    val user_table = sc.cassandraTable("tutorial", "user")

    // do something with table
    val food_index = user_table.map(r => new FoodToUserIndex(r.getString("favorite_food"), r.getString("name")))

    // save back to cass

    food_index.saveToCassandra("tutorial",
                               "food_to_user_index",
                               SomeColumns("food", "user"),
                               writeConf = WriteConf(ttl = TTLOption.constant(100)))  // that's 100 seconds ttl

    // validate: select * from food_to_user_index;

  }


}
/**
  * data for cassandra:
  *
CREATE KEYSPACE tutorial WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

use tutorial;

CREATE TABLE tutorial.user (
name text primary key,
favorite_food text
);

create table tutorial.food_to_user_index ( food text, user text, primary key (food, user));

insert into user (name, favorite_food) values ('Jon', 'bacon');
insert into user (name, favorite_food) values ('Luke', 'steak');
insert into user (name, favorite_food) values ('Al', 'salmon');
insert into user (name, favorite_food) values ('Chris', 'chicken');
insert into user (name, favorite_food) values ('Rebecca', 'bacon');
insert into user (name, favorite_food) values ('Patrick', 'brains');
insert into user (name, favorite_food) values ('Duy Hai', 'brains');
  */