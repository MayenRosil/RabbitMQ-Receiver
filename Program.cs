using System;
using System.Text;
using System.IO;
using System.Threading;


using RabbitMQ.Client;
using RabbitMQ.Client.Events;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using static System.Net.Mime.MediaTypeNames;

namespace RabbitReceiver
{
    internal class Program
    {
        //Metodo principal que levanta ambos hilos
        static void Main(string[] args)
        {
            Console.WriteLine("SUPERVISOR DE COLAS");
            
            //Levanta la cola1 en el hilo1
            ThreadStart cola1 = new ThreadStart(Cola1);
            Thread colita1 = new Thread(cola1);
            colita1.Start();

            //Levanta la cola2 en el hilo2
            ThreadStart cola2 = new ThreadStart(Cola2);
            Thread colita2 = new Thread(cola2);
            colita2.Start();


            Console.ReadKey();
        }

        //Ejecuta el listener de la cola1
        static void Cola1()
        {
            openListener("cola1");

        }

        //Ejecuta el listener de la cola2
        static void Cola2()
        {
            openListener("cola2");

        }

        //funcion para levantar el listener, segun que cola esta recibiendo
        static void openListener(string queueName)
        {
            ConnectionFactory factory = new ConnectionFactory();
            //Conexion al servidor de Rabbit con credenciales
            factory.Uri = new Uri("amqp://test:umg2023@50.17.46.30");
            factory.ClientProvidedName = "Rabbit Receiver "+ queueName;

            Console.WriteLine($"La cola {queueName} esta escuchando");

            IConnection cnn = factory.CreateConnection();
            IModel channel = cnn.CreateModel();

            //Define la configuracion del servidor
            string exchangeName = queueName;
            string routingKey = "demo-routing-key";

            channel.ExchangeDeclare(exchangeName, ExchangeType.Direct);
            channel.QueueDeclare(queueName, false, false, false, null);
            channel.QueueBind(queueName, exchangeName, routingKey, null);
            channel.BasicQos(0, 1, false);

            //Recibe el Evento de cuando la cola recibe el mensaje
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += async (sender, vars) =>
            {
                var body = vars.Body.ToArray();
                string message = Encoding.UTF8.GetString(body);

                if (queueName == "cola1")
                {
                    Console.BackgroundColor = ConsoleColor.DarkRed;
                    Console.ForegroundColor = ConsoleColor.White;
                }
                else
                {
                    Console.BackgroundColor = ConsoleColor.White;
                    Console.ForegroundColor = ConsoleColor.DarkRed;
                }
                //Dibuja el mensaje recibido en la cola
                Console.WriteLine($"MENSAJE RECIBIDO EN {queueName}: {message}");

                channel.BasicAck(vars.DeliveryTag, false);

                //Si el mensaje va a la cola1, reenvia 8 veces el mensaje a la cola2
                if(queueName == "cola1")
                {
                    for(int i = 0; i < 8; i++)
                    {
                        sendMessage(message);
                    }
                }

                //Recibir archivo base64
                try
                {   
                    //Valida si el mensaje contiene un JSON 
                    bool traeArchivoBase64 = message.Contains("\"img\":\"data");
                    if (traeArchivoBase64)
                    {

                        //Des-serializa el string JSON y lo convierte a un objeto
                        JObject jsonObject = JsonConvert.DeserializeObject<JObject>(message);

                        //Obtiene las propiedades obtenidas en el objeto JSON
                        string user = jsonObject["user"].Value<string>();
                        string pass = jsonObject["pass"].Value<string>();
                        string img = jsonObject["img"].Value<string>();

                        //Obtiene el base64 como tal, del objeto JSON
                        string[] parts = img.Split(',');
                        string part2 = parts[1];

                        //Valida si existe el folder para guardar la imagen, sino lo crea
                        if (!Directory.Exists("C:\\descargasRabbit\\"))
                        {
                            Directory.CreateDirectory("C:\\descargasRabbit\\");
                            Console.WriteLine("Folder created successfully.");
                        }

                        //Convierte el base64 a un array de bytes
                        byte[] imgByteArray = Convert.FromBase64String(part2);
                        //guarda la imagen en el folder 
                        File.WriteAllBytes("C:\\descargasRabbit\\" + $"{user}.png", imgByteArray);
                        Console.WriteLine("Imagen guardada.");
                    }
                    else
                    {

                    }
                }
                catch (JsonException ex)
                {
                    Console.WriteLine("Imagen no pudo ser guardada por motivo: " + ex);
                }
            };

            //Cierra la conexion del receiver
            string comsumerTag = channel.BasicConsume(queueName, false, consumer);
            while (true)
            {
                if (Console.KeyAvailable)
                {
                    ConsoleKeyInfo keyInfo = Console.ReadKey();
                    if (keyInfo.Key == ConsoleKey.Enter)
                    {
                        channel.BasicCancel(comsumerTag);
                        channel.Close();
                        cnn.Close();
                        break; // Sale del bucle y finaliza la aplicación.
                    }
                }
            }
        }

        //Levanta el sender, para enviar a la cola2 lo que recibe la cola1
        static bool sendMessage(string receivedMessage)
        {
            //Conexion al servidor de Rabbit con credenciales
            ConnectionFactory factory = new ConnectionFactory();
            factory.Uri = new Uri("amqp://test:umg2023@50.17.46.30");
            factory.ClientProvidedName = "Rabbit Sender App";

            IConnection cnn = factory.CreateConnection();
            IModel channel = cnn.CreateModel();

            //Define la configuracion del servidor
            string exchangeName = "cola2";
            string routingKey = "demo-routing-key";
            string queueName = "cola2";

            channel.ExchangeDeclare(exchangeName, ExchangeType.Direct);
            channel.QueueDeclare(queueName, false, false, false, null);
            channel.QueueBind(queueName, exchangeName, routingKey, null);

            //Envia el mensaje recibido, de vuelta a la cola2, con la diferenciacion
            try
            {
                byte[] messageBodyBytes = Encoding.UTF8.GetBytes("Cola 1 dice: " + receivedMessage);
                channel.BasicPublish(exchangeName, routingKey, null, messageBodyBytes);

                return true;
            }
            catch (Exception ex)
            {
                return false;
            }finally {
                //Cierra la conexion del sender
                channel.Close();
                cnn.Close();
            }


        }
    }
}
