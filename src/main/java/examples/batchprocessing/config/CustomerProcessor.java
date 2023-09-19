package examples.batchprocessing.config;

import examples.batchprocessing.entities.Customer;
import org.springframework.batch.item.ItemProcessor;

import java.text.SimpleDateFormat;
import java.time.*;
import java.util.Date;

public class CustomerProcessor implements ItemProcessor<Customer,Customer> {

    @Override
    public Customer process(Customer item) throws Exception {

        Integer age = item.getAge();
        //Instantiating the SimpleDateFormat class
//        SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
//        //Parsing the given String to Date object
//        Date date = (Date) formatter.parseObject(dob);
//        System.out.println("Date object value: "+date);
//
//        //Converting obtained Date object to LocalDate object
//        Instant instant = date.toInstant();
//        ZonedDateTime zone = instant.atZone(ZoneId.systemDefault());
//        LocalDate givenDate = zone.toLocalDate();
//
//        Period period = Period.between(givenDate, LocalDate.now());

        if (age>18){
            return item;
        }
    return null;
    }
}
