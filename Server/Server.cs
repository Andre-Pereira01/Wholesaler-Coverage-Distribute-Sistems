using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

class Servidor
{
    private static Mutex mutexPrincipal = new Mutex();
    private static Dictionary<string, Mutex> mutexEmpresa = new Dictionary<string, Mutex>();
    private static int clientesConectados = 0;
    private static Dictionary<string, List<string>> empresas = new Dictionary<string, List<string>>();
    private static readonly string arquivoLog = "uploads.log";
    public static void Main()
    {
        Console.OutputEncoding = Encoding.UTF8;

        CarregarCSV();

        IPAddress ip = IPAddress.Parse("127.0.0.1");
        TcpListener servidor = new TcpListener(ip, 8888);

        servidor.Start();

        Console.WriteLine("Servidor iniciado...");
        while (true)
        {
            TcpClient cliente = servidor.AcceptTcpClient();
            Thread threadCliente = new Thread(new ParameterizedThreadStart(ProcessarCliente));
            threadCliente.Start(cliente);
        }
    }

    private static void CarregarCSV()
    {
        string[] arquivos = Directory.GetFiles(".", "*.csv");

        foreach (string arquivo in arquivos)
        {
            string empresa = Path.GetFileNameWithoutExtension(arquivo);
            List<string> linhas = File.ReadAllLines(arquivo, Encoding.UTF8).ToList();
            empresas.Add(empresa, linhas);
            mutexEmpresa.Add(empresa, new Mutex());
        }
    }

    private static void ProcessarCliente(object clienteObj)
    {
        TcpClient cliente = (TcpClient)clienteObj;
        NetworkStream fluxoDados = cliente.GetStream();
        StreamReader fluxoLeitura = new StreamReader(fluxoDados, Encoding.UTF8);
        StreamWriter fluxoEscrita = new StreamWriter(fluxoDados, Encoding.UTF8) { AutoFlush = true };

        mutexPrincipal.WaitOne();
        int idClienteAtual = ++clientesConectados;
        mutexPrincipal.ReleaseMutex();

        Console.WriteLine($"Cliente {idClienteAtual} conectado.");

        string empresa = null;
        Mutex mutexEmpresaAtual = null;

        try
        {
            fluxoEscrita.WriteLine("100 OK");

            while (true)
            {
                string comandoRecebido = fluxoLeitura.ReadLine();

                if (comandoRecebido == "QUIT")
                {
                    if (mutexEmpresaAtual != null)
                    {
                        mutexEmpresaAtual.ReleaseMutex();
                        fluxoEscrita.WriteLine("400 BYE");
                        break;
                    }
                }
                else if (comandoRecebido == "SEND")
                {
                    if (mutexEmpresaAtual != null)
                    {
                        mutexEmpresaAtual.ReleaseMutex();
                    }

                    empresa = fluxoLeitura.ReadLine();

                    if (!empresas.ContainsKey(empresa))
                    {
                        mutexPrincipal.WaitOne();
                        empresas.Add(empresa, new List<string>());
                        mutexEmpresa.Add(empresa, new Mutex());
                        mutexPrincipal.ReleaseMutex();
                    }
                    mutexEmpresaAtual = mutexEmpresa[empresa];
                    mutexEmpresaAtual.WaitOne();

                    string nomeArquivo = fluxoLeitura.ReadLine();

                    List<string> linhasRecebidas = new List<string>();
                    string linhaAtual;
                    while ((linhaAtual = fluxoLeitura.ReadLine()) != null && linhaAtual != "END")
                    {
                        linhasRecebidas.Add(linhaAtual);
                    }

                    linhasRecebidas.RemoveAt(0);

                    string caminhoCSV = $"{empresa}.csv";

                    if (!File.Exists(caminhoCSV))
                    {
                        Console.WriteLine($"Arquivo {caminhoCSV} nao encontrado. Criando novo arquivo.");
                        File.WriteAllText(caminhoCSV, "Operadora, Localidade, Domicílios, Município\n");
                    }
                    else
                    {
                        Console.WriteLine($"Arquivo {caminhoCSV} encontrado.");
                    }
                    if (empresas.ContainsKey(empresa))
                    {
                        var linhasExistentes = empresas[empresa];
                        linhasExistentes.AddRange(linhasRecebidas.Where(linha => !linhasExistentes.Contains(linha)));
                        linhasExistentes.Sort((x, y) => x.Split(',')[3].CompareTo(y.Split(',')[3]));
                        File.WriteAllLines(caminhoCSV, linhasExistentes, Encoding.UTF8);
                    }
                    else
                    {
                        List<string> linhasOrdenadas = new List<string> { "cod_distrito,cod_concelho,cod_localidade,nome_localidade,cod_arteria,tipo_arteria,prep1,titulo_arteria,prep2,nome_arteria,local_arteria,troco,porta,cliente,num_cod_postal,ext_cod_postal,desig_postal" };
                        linhasOrdenadas.AddRange(linhasRecebidas);
                        linhasOrdenadas.Sort((x, y) => x.Split(',')[3].CompareTo(y.Split(',')[3]));
                        File.WriteAllLines(caminhoCSV, linhasOrdenadas, Encoding.UTF8);
                        empresas.Add(empresa, linhasOrdenadas);
                    }

                    RegistrarUpload(empresa, nomeArquivo, idClienteAtual);

                    Console.WriteLine($"Arquivo CSV processado para a empresa {empresa}.");

                    var localidades = empresas[empresa]
                        .Select(linha => linha.Split(','))
                        .GroupBy(arr => arr[3])
                        .Select(g => new { Localidade = g.Key, Domicílios = g.Count() });

                    Console.WriteLine("Resumo das localidades:");
                    foreach (var localidade in localidades)
                    {
                        string saida = $"Localidade: {localidade.Localidade}, Domicilios: {localidade.Domicílios}";
                        Console.WriteLine(saida);
                        fluxoEscrita.WriteLine(saida);
                    }

                    int sobreposicoes = ContarSobreposicoes(empresas[empresa], linhasRecebidas);

                    var sobreposicoesPorLocalidade = linhasRecebidas
                        .Select(linha => linha.Split(','))
                        .GroupBy(arr => arr[3])
                        .Select(g => new { Localidade = g.Key, Sobreposicoes = g.Sum(arr => ContarSobreposicoes(empresas[empresa], new List<string> { string.Join(",", arr) })) });

                    Console.WriteLine("Sobreposiçoes por município:");
                    foreach (var sobreposicao in sobreposicoesPorLocalidade)
                    {
                        string saida = $"Localidade: {sobreposicao.Localidade}, Sobreposiçoes: {sobreposicao.Sobreposicoes}";
                        Console.WriteLine(saida);
                        fluxoEscrita.WriteLine(saida);
                    }
                    fluxoEscrita.WriteLine("400 BYE");
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erro: {ex.Message}");
        }

        Console.WriteLine($"Cliente {idClienteAtual} desconectado.");
        fluxoDados.Close();
        cliente.Close();
    }

    private static int ContarSobreposicoes(List<string> linhasServidor, List<string> linhasCliente)
    {
        var domiciliosServidor = linhasServidor
            .Select(linha => linha.Split(','))
            .GroupBy(arr => new
            {
                Localidade = arr[3],
                CodArteria = arr[4],
                Porta = arr[12]
            }).ToDictionary(g => g.Key, g => g.Count());
        int sobreposicoes = 0;

        foreach (var linha in linhasCliente)
        {
            var arr = linha.Split(',');
            var chave = new { Localidade = arr[3], CodArteria = arr[4], Porta = arr[12] };

            if (domiciliosServidor.ContainsKey(chave))
            {
                sobreposicoes += domiciliosServidor[chave];
            }
        }

        return sobreposicoes;
    }

    private static void RegistrarUpload(string empresa, string nomeArquivo, int idCliente)
    {
        string dataUpload = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        string entradaLog = $"{empresa}, {dataUpload}, {nomeArquivo}, {idCliente}\n";

        if (!File.Exists(arquivoLog))
        {
            File.WriteAllText(arquivoLog, "Operadora, Data Upload, Nome Arquivo, ID Cliente\n", Encoding.UTF8);
        }

        File.AppendAllText(arquivoLog, entradaLog, Encoding.UTF8);
    }
}


