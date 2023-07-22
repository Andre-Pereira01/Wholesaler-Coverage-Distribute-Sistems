using System;
using System.IO;
using System.Net.Sockets;
using System.Text;

class ClienteEnvioArquivo
{
    public static void Main()
    {
        Console.OutputEncoding = Encoding.UTF8;
        Console.WriteLine("Iniciando conexao");
        using (TcpClient conexao = new TcpClient("127.0.0.1", 8888))
        {
            Console.WriteLine("Conexao estabelecida.");

            using (NetworkStream fluxo = conexao.GetStream())
            {
                StreamReader leitor = new StreamReader(fluxo, Encoding.UTF8);
                StreamWriter escritor = new StreamWriter(fluxo, Encoding.UTF8) { AutoFlush = true };

                string respostaServidor = leitor.ReadLine();
                Console.WriteLine($"Resposta do servidor: {respostaServidor}");

                if (respostaServidor == "100 OK")
                {
                    while (true)
                    {
                        Console.WriteLine("Digite o nome da operadora desejada ou QUIT para encerrar:");
                        string empresa = Console.ReadLine();

                        if (empresa.ToUpper() == "QUIT")
                        {
                            escritor.WriteLine("QUIT");
                            break;
                        }

                        escritor.WriteLine("SEND");
                        escritor.WriteLine(empresa);

                        Console.WriteLine("Informe o caminho do arquivo CSV:");
                        string caminhoArquivo = Console.ReadLine();

                        if (File.Exists(caminhoArquivo))
                        {
                            string[] linhas = File.ReadAllLines(caminhoArquivo, Encoding.UTF8);
                            escritor.WriteLine(Path.GetFileName(caminhoArquivo));

                            Console.WriteLine($"Enviando CSV {Path.GetFileName(caminhoArquivo)}...");
                            foreach (string linha in linhas)
                            {
                                escritor.WriteLine(linha);
                            }
                            escritor.WriteLine("END");
                            Console.WriteLine("CSV enviado. Aguardando processamento...");

                            string informacoesMunicipio;
                            while ((informacoesMunicipio = leitor.ReadLine()) != "400 BYE")
                            {
                                if (informacoesMunicipio == "500 OPERADORA NAO ENCONTRADA")
                                {
                                    Console.WriteLine("Operadora nao encontrada.");
                                    break;
                                }
                                Console.WriteLine(informacoesMunicipio);
                            }
                        }
                        else
                        {
                            Console.WriteLine("Arquivo CSV nao encontrado.");
                        }
                    }
                }
            }
        }

        Console.WriteLine("400 BYE");
    }
}