#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <pthread.h>
#include <unistd.h>

// Funcao de ordenacao fornecida. Não pode alterar.
void bubble_sort(int *v, int tam){
    int i, j, temp, trocou;

    for(j = 0; j < tam - 1; j++){
        trocou = 0;
        for(i = 0; i < tam - 1; i++){
            if(v[i + 1] < v[i]){
                temp = v[i];
                v[i] = v[i + 1];
                v[i + 1] = temp;
                trocou = 1;
            }
        }
        if(!trocou) break;
    }
}

// Funcao para imprimir um vetor. Não pode alterar.
void imprime_vet(unsigned int *v, int tam) {
    int i;
    for(i = 0; i < tam; i++)
        printf("%d ", v[i]);
    printf("\n");
}

// Funcao para ler os dados de um arquivo e armazenar em um vetor em memoroa. Não pode alterar.
int le_vet(char *nome_arquivo, unsigned int *v, int tam) {
    FILE *arquivo;
    
    // Abre o arquivo
    arquivo = fopen(nome_arquivo, "r");
    if (arquivo == NULL) {
        perror("Erro ao abrir o arquivo");
        return 0;
    }

    // Lê os números
    for (int i = 0; i < tam; i++)
        fscanf(arquivo, "%u", &v[i]);

    fclose(arquivo);

    return 1;
}

// Funcao principal de ordenacao. Deve ser implementada com base nas informacoes fornecidas no enunciado do trabalho.
// Os numeros ordenados deverao ser armazenanos no proprio "vetor".
int sort_paralelo(unsigned int *vetor, unsigned int tam, unsigned int ntasks, unsigned int nthreads) {
    if (ntasks > tam || nthreads < 1) {
        fprintf(stderr, "número inválido\n");
        return 0;
    }

    unsigned int intervalo = tam / ntasks; // quantidade base de cada faixa
    unsigned int resto = tam % ntasks; // resto se divisão nao for exata

    unsigned int minimo[ntasks]; // valor minimo da tarefa
    unsigned int maximo[ntasks]; // valor maximo da tarefa
    unsigned int tamanho[ntasks]; // tamanho atual da tarefa
    unsigned int* dados[ntasks]; // ponteiro p/ vetor com os valores da tarefa

    unsigned int inicio = 0; // valor minimo da faixa
    for (unsigned int i = 0; i < ntasks; i++) { // percorretodas asa tarefas (0 a ntasks-1)
        unsigned int extra; // calcula se a tarefa deve receber 1 valor extra 
        if (i < resto) {
            extra = 1;
        } else {
            extra = 0;
        } 

        unsigned int fim = inicio + intervalo + extra; // calcula valor final da faixa

        minimo[i] = inicio; //salva no vetor o valor inicial
        maximo[i] = fim; // salva no vetor o valor final
        tamanho[i] = 0; // inicaliza com 0 - nenhum valor inserido ainda

        dados[i] = malloc(tam * sizeof(unsigned int)); // aloca memoria para valores da tarefa

        inicio = fim; // atualiza a proxima tarefa começar pelo fim da anterior
    }

    for (unsigned int i = 0; i < tam; i++) { // percorre vetor desordenado
        unsigned int valor = vetor[i];

        for (unsigned int j = 0; j < ntasks; j++) { // percorre as tarefas 
            if (valor >= minimo[j] && valor < maximo[j]) { // verifica se o vlor pertence a faixa 'j' 
                dados[j][tamanho[j]] = valor; // insere na prox posicao livre
                tamanho[j]++; // aumenta contador
                break;
            }
        }
    }
}

// Funcao principal do programa. Não pode alterar.
int main(int argc, char **argv) {
    
    // Verifica argumentos de entrada
    if (argc != 5) {
        fprintf(stderr, "Uso: %s <input> <nnumbers> <ntasks> <nthreads>\n", argv[0]);
        return 1;
    }

    // Argumentos de entrada
    unsigned int nnumbers = atoi(argv[2]);
    unsigned int ntasks = atoi(argv[3]);
    unsigned int nthreads = atoi(argv[4]);
    
    // Aloca vetor
    unsigned int *vetor = malloc(nnumbers * sizeof(unsigned int));

    // Variaveis de controle de tempo de ordenacao
    struct timeval inicio, fim;

    // Le os numeros do arquivo de entrada
    if (le_vet(argv[1], vetor, nnumbers) == 0)
        return 1;

    // Imprime vetor desordenado
    imprime_vet(vetor, nnumbers);

    // Ordena os numeros considerando ntasks e nthreads
    gettimeofday(&inicio, NULL);
    sort_paralelo(vetor, nnumbers, ntasks, nthreads);
    gettimeofday(&fim, NULL);

    // Imprime vetor ordenado
    imprime_vet(vetor, nnumbers);

    // Desaloca vetor
    free(vetor);

    // Imprime o tempo de ordenacao
    printf("Tempo: %.6f segundos\n", fim.tv_sec - inicio.tv_sec + (fim.tv_usec - inicio.tv_usec) / 1e6);
    
    return 0;
}