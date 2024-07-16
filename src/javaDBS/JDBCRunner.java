package javaDBS;

import java.sql.*;

public class JDBCRunner {
    private static final String PROTOCOL = "jdbc:postgresql://";        // URL-prefix
    private static final String DRIVER = "org.postgresql.Driver";       // Driver name
    private static final String URL_LOCALE_NAME = "localhost/";         // ваш компьютер + порт по умолчанию
    private static final String URL_REMOTE = "192.168.0.26/";           // IP-адрес кафедрального сервера + явно порт (по умолчанию)

    private static final String DATABASE_NAME = "postgres_air";          // FIXME имя базы

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";                  // FIXME имя пользователя
    public static final String DATABASE_PASS = "12345";                 // FIXME пароль базы данных


    public static void main(String[] args) {
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных| " + DATABASE_URL + "\n");

        // попытка открыть соединение с базой данных, которое Java закроет перед выходом из try-with-resources
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            getAccount(connection);   System.out.println("1"); // выполняет заброс к таблице account и выводит результаты
            getAircraft(connection);  System.out.println("2"); //выполняет заброс к таблице aircraft и выводит результаты
            getAirport(connection);    System.out.println("3"); //выполняет заброс к таблице airport и выводит результаты
            getBoardingPass(connection); System.out.println("4"); //выполняет заброс к таблице aircraft и выводит результаты
            getBooking(connection);      System.out.println("5"); //выполняет заброс к таблице booking и выводит результаты
            getDELETE12(connection); System.out.println("6"); // удаляет
//            getBookingLeg(connection);  System.out.println("7"); //выполняет заброс к таблице BookingLeg и выводит результаты
//            getFlight(connection); System.out.println("8"); //выполняет заброс к таблице flight и выводит результаты
//            getFrequentFlyer(connection); System.out.println("9"); //выполняет заброс к таблице frequentFlyer и выводит результаты
//            getPassenger(connection); System.out.println("10"); //выполняет заброс к таблице passenger и выводит результаты
//            getPhone(connection); System.out.println("11"); //выполняет заброс к таблице phone и выводит результаты
           getFilingNO(connection); System.out.println("12"); // JOIN
            // блок демонстрации изменения
            getSELECT(connection); System.out.println("Общий SELEСT  по фамилии BENETSKAYA если есть"); //
            getDELETEALL(connection); System.out.println("Удалили всех записи  KATYA BENETSKAYA если есть");
            getSELECT(connection);System.out.println("Общий SELEСT  по фамилии BENETSKAYA");
            getINSERTINTO12(connection);System.out.println("Добавили KATYA 12 yes");
            getINSERTINTO15(connection);System.out.println("Добавили KATYA 15 yes");
            getINSERTINTO20(connection);System.out.println("Добавили KATYA 20 yes");
            getINSERTINTO15(connection);System.out.println("Добавили KATYA 15 yes");
            getSELECT(connection);System.out.println("Общий SELEСT  по фамилии BENETSKAYA");
            getUPDATE(connection); System.out.println(" Изменили возраст BENETSKAYA с 20-ти на 9 ");
            getSELECT(connection);System.out.println("Общий SELEСT  по фамилии BENETSKAYA");
            // блок удаления демонстрация

          addPassenger(connection,168732,"BENETSKAYA","KATYA");
        } catch (SQLException e) {
            if (e.getSQLState().startsWith("23")) { //
                System.out.println("Произошло дублирование данных"); //
            } else throw new RuntimeException(e); //
        }
    }

//Проверка окружения и доступа к базе данных
// Основная функция программы. Вызывает функции
// для проверки драйвера и базы данных, затем устанавливает соединение
// с базой данных и вызывает функции для получения информации из различных таблиц базы данных.

    private static void checkDriver() { //
        try {
            Class.forName(DRIVER); //
        } catch (ClassNotFoundException e) { //
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e); //
        }
    }

    //  Функция для проверки наличия JDBC-драйвера. В случае ошибки выбрасывает исключение и выводит сообщение об отсутствии драйвера.
    private static void checkDB() { //
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS); //
        } catch (SQLException e) { //
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e); //
        }
    }
    // region // SELECT-запросы без параметров в одной таблице

    //Функция для проверки наличия базы данных. В случае ошибки выбрасывает исключение и выводит сообщение об отсутствии базы данных.
    private static void getAccount(Connection connection) throws SQLException {
        // имена столбцов
        String columnName0 = "account_id", columnName1 = "login", columnName2 = "first_name", columnName3 = "last_name", columnName4 = "frequent_flyer_id", columnName5 = "update_ts";
        // значения ячеек
        int param0 = -1;
        String param1 = null, param2 = null, param3 = null, param4 = null, param5 = null;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM postgres_air.account ACS LIMIT 15;"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные, продвигаться по ним
            param0 = rs.getInt(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            param1 = rs.getString(columnName1);
            param2 = rs.getString(columnName2);
            param3 = rs.getString(columnName3);
            param4 = rs.getString(columnName4);
            param5 = rs.getString(columnName5); // значение ячейки, можно получить по имени; по умолчанию возвращается строка
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5);
        }
    }

    private static void getAircraft(Connection connection) throws SQLException {
        // значения ячеек
        String param0 = null, param1 = null, param2 = null, param3 = null, param4 = null;

        Statement statement = connection.createStatement();                 // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM postgres_air.aircraft;");  // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные
            param0 = rs.getString(1);
            param1 = rs.getString(2);
            param2 = rs.getString(3);
            param3 = rs.getString(4);
            param4 = rs.getString(5);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4);
        }
    }

    private static void getAirport(Connection connection) throws SQLException {
        // значения ячеек
        String param0 = null, param1 = null, param2 = null, param3 = null, param4 = null, param5 = null, param6 = null, param7 = null;

        Statement statement = connection.createStatement();             // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM postgres_air.airport ACS LIMIT 15;");   // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные
            param0 = rs.getString(1);
            param1 = rs.getString(2);
            param2 = rs.getString(3);
            param3 = rs.getString(4);
            param4 = rs.getString(5);
            param5 = rs.getString(6);
            param6 = rs.getString(7);
            param7 = rs.getString(8); // значение ячейки, можно получить по имени
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6 + " | " + param7);
        }
    }

    private static void getBoardingPass(Connection connection) throws SQLException {
        // значения ячеек
        String param0 = null, param1 = null, param2 = null, param3 = null, param4 = null, param5 = null, param6 = null;

        Statement statement = connection.createStatement();             // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM postgres_air.boarding_pass  ACS LIMIT 15;");   // выполняем запроса на поиск и получаем список ответов
        System.out.println("Печать первых 15-ти посадочных талонов :");
        while (rs.next()) {  // пока есть данные
            param0 = rs.getString(1);
            param1 = rs.getString(2);
            param2 = rs.getString(3);
            param3 = rs.getString(4);
            param4 = rs.getString(5);
            param5 = rs.getString(6);
            param6 = rs.getString(7);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6);
        }
    }

    private static void getBooking(Connection connection) throws SQLException {
        // значения ячеек
        String param0 = null, param1 = null, param2 = null, param3 = null, param4 = null, param5 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM postgres_air.booking  ACS LIMIT 15;");   // выполняем запроса на поиск и получаем список ответов
        System.out.println("Печать первых 15-ти букингов :");
        while (rs.next()) {  // пока есть данные
            param0 = rs.getString(1);
            param1 = rs.getString(2);
            param2 = rs.getString(3);
            param3 = rs.getString(4);
            param4 = rs.getString(5);
            param5 = rs.getString(6);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5);
        }
    }

    private static void getBookingLeg(Connection connection) throws SQLException {
        // значения ячеек
        String param0 = null, param1 = null, param2 = null, param3 = null, param4 = null;

        Statement statement = connection.createStatement();             // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM postgres_air.booking_leg  ACS LIMIT 15;");   // выполняем запроса на поиск и получаем список ответов
        System.out.println("Печать первых 15-ти сегментов бронирования :");
        while (rs.next()) {  // пока есть данные
            param0 = rs.getString(1);
            param1 = rs.getString(2);
            param2 = rs.getString(3);
            param3 = rs.getString(4);
            param4 = rs.getString(5);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4);
        }
    }

    private static void getFlight(Connection connection) throws SQLException {
        // значения ячеек
        String param0 = null, param1 = null, param2 = null, param3 = null, param4 = null, param5 = null, param6 = null, param7 = null, param8 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM postgres_air.flight  ACS LIMIT 15;");
        System.out.println("Печать информации о  первых 15-ти рейсах :");
        while (rs.next()) {  // пока есть данные
            param0 = rs.getString(1);
            param1 = rs.getString(2);
            param2 = rs.getString(3);
            param3 = rs.getString(4);
            param4 = rs.getString(5);
            param5 = rs.getString(6);
            param6 = rs.getString(7);
            param7 = rs.getString(8);
            param8 = rs.getString(9);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6 + " | " + param7 + " | " + param8);
        }
    }

    private static void getFrequentFlyer(Connection connection) throws SQLException {
        // значения ячеек
        String param0 = null, param1 = null, param2 = null, param3 = null, param4 = null, param5 = null, param6 = null, param7 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM postgres_air.frequent_flyer  ACS LIMIT 15;");
        System.out.println("Печать информации о членстве в программе бонусов для часто летающих пассажиров  :");
        while (rs.next()) {  // пока есть данные
            param0 = rs.getString(1);
            param1 = rs.getString(2);
            param2 = rs.getString(3);
            param3 = rs.getString(4);
            param4 = rs.getString(5);
            param5 = rs.getString(6);
            param6 = rs.getString(7);
            param7 = rs.getString(8);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6 + " | " + param7);
        }
    }

    private static void getPassenger(Connection connection) throws SQLException {
        // значения ячеек
        String param0 = null, param1 = null, param2 = null, param3 = null, param4 = null, param5 = null, param6 = null, param7 = null;

        Statement statement = connection.createStatement();             // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM postgres_air.passenger  ACS LIMIT 15;");   // выполняем запроса на поиск и получаем список ответов
        System.out.println("Печать информации о первых 15-ти пассажирах:");
        while (rs.next()) {  // пока есть данные
            param0 = rs.getString(1);
            param1 = rs.getString(2);
            param2 = rs.getString(3);
            param3 = rs.getString(4);
            param4 = rs.getString(5);
            param5 = rs.getString(6);
            param6 = rs.getString(7);
            param7 = rs.getString(8);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6 + " | " + param7);
        }
    }

    private static void getPhone(Connection connection) throws SQLException {
        // значения ячеек
        String param0 = null, param1 = null, param2 = null, param3 = null, param4 = null, param5 = null;

        Statement statement = connection.createStatement();             // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM postgres_air.phone  ACS LIMIT 15;");   // выполняем запроса на поиск и получаем список ответов
        System.out.println("Печать первых 15-ти номеров телефонов пассажиров :");
        while (rs.next()) {  // пока есть данные
            param0 = rs.getString(1);
            param1 = rs.getString(2);
            param2 = rs.getString(3);
            param3 = rs.getString(4);
            param4 = rs.getString(5);
            param5 = rs.getString(6);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5);
        }
    }

    public static void getFilingNO(Connection connection) throws SQLException { // сколько пассажиров совершило перелет между двумя аэропортами в период с 2023-08-01 до 2023-08-15
        int param0 = -1;
        String param1 = null, param2 = null, param3 = null, param4 = null, param5 = null;
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT f.flight_no,f.actual_departure,\n" +
                "count(passenger_id) passengers\n" +
                "FROM postgres_air.flight f\n" +
                "JOIN postgres_air.booking_leg bl ON bl.flight_id = f.flight_id\n" +
                "JOIN postgres_air.passenger p ON p.booking_id=bl.booking_id\n" +
                "WHERE f.departure_airport = 'JFK'\n" +
                "AND f.arrival_airport = 'ORD'\n" +
                "AND f.actual_departure BETWEEN '2023-08-01' AND '2023-08-15'\n" +
                "GROUP BY f.flight_id, f.actual_departure;\n");
        System.out.println("Печать количества пассажиров,совершивших перелет между  аэропортами в период с 2023-08-01 до 2023-08-15 :");
        while (rs.next()) {  // пока есть данные
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getString(3);

            System.out.println(param0 + " | " + param1 + " | " + param2);
        }
    }

    public static void getSELECT(Connection connection) throws SQLException {  // функцию вывода всех строк, где фамилия BENETSKAYA - смотрим, что нет строки
        int param2, param3 = -1;
        String param0 = null, param1 = null;
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("\tSELECT  passenger_id, last_name, first_name, age FROM postgres_air.passenger\n" +
                "WHERE last_name = 'BENETSKAYA'\n" +
                "\n");
        System.out.println("Выбор по фамилии BENETSKAYA");
        while (rs.next()) {  // пока есть данные
            param3 = rs.getInt(1);
            param0 = rs.getString(2);
            param1 = rs.getString(3);
            param2 = rs.getInt(4);


            System.out.println(param3 + " | " + param0 + " | " + param1 + " | " + param2);

            // endregion
        }
    }

    public static void getINSERTINTO12(Connection connection) throws SQLException { //
        // функцию добавления строки с фамилией Benetskaya - ничего не выводит, просто добавляет строк. Функция вывода всех строк, где фамилия BENETSKAYA - увидели, что строка добавилась
        Statement statement = connection.createStatement();
        statement.executeUpdate("INSERT INTO postgres_air.passenger(booking_id, first_name, last_name, age)\n" +
                "VALUES ( '159', 'EKATERINA', 'BENETSKAYA', '12');");

    }

    public static void getINSERTINTO20(Connection connection) throws SQLException { //
        // функцию добавления строки с фамилией Benetskaya - ничего не выводит, просто добавляет строк. Функция вывода всех строк, где фамилия BENETSKAYA - увидели, что строка добавилась
        Statement statement = connection.createStatement();
        statement.executeUpdate("INSERT INTO postgres_air.passenger(booking_id, first_name, last_name, age)\n" +
                "VALUES ( '154878', 'EKATERINA', 'BENETSKAYA', '20');");

    }

    public static void getINSERTINTO15(Connection connection) throws SQLException { //
        // функцию добавления строки с фамилией Benetskaya - ничего не выводит, просто добавляет строк. Функция вывода всех строк, где фамилия BENETSKAYA - увидели, что строка добавилась
        Statement statement = connection.createStatement();
        statement.executeUpdate("INSERT INTO postgres_air.passenger(booking_id, first_name, last_name, age)\n" +
                "VALUES ( '15474', 'EKATERINA', 'BENETSKAYA', '15');");

    }


//
//    public static void getSELECT1(Connection connection) throws SQLException {  // функцию вывода всех строк, где фамилия BENETSKAYA - смотрим, что нет строки
//        int param2 = -1;
//        String param0 = null, param1 = null, param3 = null, param4 = null, param5 = null;
//        Statement statement = connection.createStatement();
//        ResultSet rs = statement.executeQuery("\tSELECT  last_name, first_name, age from postgres_air.passenger\n" +
//                "WHERE last_name = 'BENETSKAYA'\n" +
//                "\n");
//
//        // endregion
//    }


    public static void getDELETE12(Connection connection) throws SQLException { // удаление
        Statement statement = connection.createStatement();
        statement.executeUpdate("DELETE FROM postgres_air.passenger\n" +
                "\tWHERE last_name = 'BENETSKAYA' AND age = '12';");
        System.out.println("DELETE");
    }

    public static void getDELETEALL(Connection connection) throws SQLException { // удаление
        Statement statement = connection.createStatement();
        statement.executeUpdate("DELETE FROM postgres_air.passenger\n" +
                "\tWHERE last_name = 'BENETSKAYA';");
        System.out.println("DELETE");
    }


    public static void getAllPassengers(Connection connection) throws SQLException {  // функцию добавления строки с фамилией Benetskaya - ничего не выводит, просто добавляет строк. Функция вывода всех строк, где фамилия BENETSKAYA - увидели, что строка добавилась
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM postgres_air.passenger\n" +
                "\tWHERE last_name = 'BENETSKAYA'");
        System.out.println("Возвращение");
        while (resultSet.next()) {
            String lastName = resultSet.getString("last_name");
            String firstName = resultSet.getString("first_name");
            int age = resultSet.getInt("age");
            System.out.println(lastName + " | " + firstName + "|" + age);

        }
        resultSet.close();
        resultSet.close();

    }

    public static void getUPDATE(Connection connection) throws SQLException { // изменение
        Statement statement = connection.createStatement();
        statement.executeUpdate("UPDATE postgres_air.passenger SET age = 9\n" +
                "\tWHERE last_name = 'BENETSKAYA' AND age = 20");

    }

    public static void getSEARCH(Connection connection) throws SQLException {        // Функция поиска строк по фамилии
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT first_name, last_name, age from postgres_air.passenger\n" +
                "\tWHERE last_name = 'BENETSKAYA'\n" +
                "\n");
        System.out.println("Search results for " + connection + ":");
        while (rs.next()) {
            System.out.println("First_name: " + rs.getString("first_name"));
            System.out.println("Last_name: " + rs.getString("last_name"));
            System.out.println("Age: " + rs.getInt("age"));
        }
    }


    private static void addPassenger(Connection connection, int booking_id, String last_name, String first_name) throws SQLException {
        if (last_name == null || last_name.isBlank() || first_name == null || first_name.isBlank() ||
                booking_id == 0) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO postgres_air.passenger(booking_id, last_name, first_name) VALUES (?, ?, ?) returning booking_id;", Statement.RETURN_GENERATED_KEYS);    // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setInt(1, booking_id);
        statement.setString(2, last_name);
        statement.setString(3, first_name);

        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        ResultSet rs = statement.getGeneratedKeys(); // прочитать запрошенные данные из БД
        if (rs.next()) { // прокрутить к первой записи, если они есть
            System.out.println("Идентификатор регистрации пассажира " + rs.getInt(1));
        }
        System.out.println("Добавлено " + count + " пассажиров");
        getPassenger(connection);

    }

}


