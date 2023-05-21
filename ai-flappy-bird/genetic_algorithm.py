import random

class GeneticAlgorithm():
    def __init__(self, population_size, chromosome_length):
        self.population = []
        self.population_size = population_size
        self.chromosome_length = chromosome_length

    def initialize_population(self):
        for i in range (self.population_size):
            chromosome = []

            for i in range(self.chromosome_length):
                gene = random.randint(0, 1)
                chromosome.append(gene)
            
            self.population.append(chromosome)
        
        return self.population

    def fitness_function(self, distance, pipes_passed, time_survived):
        distance_weight = 0.4
        pipes_weight = 0.3
        time_weight = 0.3

        fitness_cost = (distance * distance_weight) + (pipes_passed * pipes_weight) + (time_survived * time_weight)
        return fitness_cost
    
    def parent_selection(self, population, tournament_size):
        selected_parents = []
       
        for i in range(2):
            tournament_population = random.sample(population, tournament_size) # Randomly select tournament_size individuals from the population 
            best_parent = max(tournament_population, key=lambda chromosome: self.fitness_function(chromosome[0], chromosome[1], chromosome[2])) # Find the chromosome in the tournament
            # with the max fitness value. The second part specifies the key based on which max value is determined. It uses a lambda function to calculate the fitness cost of each chromosome in the tournment population
            # The lambda function is a comparison function for the 'max' function
            selected_parents.append(best_parent)        

        return selected_parents

    def crossover(self, parent1, parent2):
        crosspoint = random.randint(1, self.chromosome_length - 1)
        print(f"\nCrosspoint for crossover is: {crosspoint}")
        child1 = parent1[:crosspoint] + parent2[crosspoint:] # parent1[:crosspoint] includes elements from index 0 to crosspoint -1, and parent2[crosspoint:] includes elements from crosspoint to the end of the list
        child2 = parent2[:crosspoint] + parent1[crosspoint:]

        return child1, child2

    def mutation(self, chromosome, prob):
        mutated_chromosome = list(chromosome)
        original_fitness = self.fitness_function(chromosome[0], chromosome[1], chromosome[2])
        print(f"Original fitness: {original_fitness}")

        for i in range(len(mutated_chromosome)):
            if random.random() < prob:
                mutated_chromosome[i] = mutated_chromosome[i] ^ 1

        mutated_fitness = self.fitness_function(mutated_chromosome[0], mutated_chromosome[1], mutated_chromosome[2])
        print(f"Mutated fitness: {mutated_fitness}")

        if mutated_fitness < original_fitness:
            return mutated_chromosome
        else:
            return chromosome

    def evaluate_fitness(self, population):
        fitness_scores = []
        for chromosome in population:
            distance = chromosome[0]
            pipes_passed = chromosome[1]
            time_survived = chromosome[2]
            fitness = self.fitness_function(distance, pipes_passed, time_survived)
            fitness_scores.append(fitness)
        
        return fitness_scores

    def evolve_population(self):
        population = self.initialize_population()
        print("Population: ")
        for chromosome in population:
            print(chromosome)

        # Evaluate fitness before crossover and mutation
        fitness_scores_before = self.evaluate_fitness(population)
        print(f"\nFitness Scores before: {fitness_scores_before}")

        # Crossover
        tournament_size = 5
        selected_parents = self.parent_selection(population, tournament_size)
        offspring_population = []
        for i in range(0, len(selected_parents), 2):
            parent1 = selected_parents[i]
            parent2 = selected_parents[i + 1]
            offspring1, offspring2 = self.crossover(parent1, parent2)
            offspring_population.append(offspring1)
            offspring_population.append(offspring2)
        for chromosome in offspring_population:
            print(chromosome)

        # Mutation
        prob = 0.1
        mutated_population = []
        for chromosome in offspring_population:
            mutated_chromosome = self.mutation(chromosome, prob)
            mutated_population.append(mutated_chromosome)
        for chromosome in mutated_population:
            print(chromosome)

        # Remove worst chromosomes
        population = population + mutated_population
        population = sorted(population, key=lambda chromosome: self.fitness_function(chromosome[0], chromosome[1], chromosome[2]))
        population = population[:-2]
        # Evaluate fitness after crossover and mutation
        fitness_scores_after = self.evaluate_fitness(mutated_population)
        print("Fitness Scores After:")
        print(fitness_scores_after)

        print("\nPopulation after mutation")
        for chromosome in population:
            print(chromosome)
        
ga = GeneticAlgorithm(10, 10)
ga.evolve_population()
