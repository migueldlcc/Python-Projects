import random
##from xmlrpc.client import boolean

"""This class creates a card in string format for the game, having a value and a suit"""
class Card(object):
    def __init__(self, value, suit): # Lets the class initialize the object's attributes
        self.value = value
        self.suit = suit
            
    def __repr__(self):
        return str(self.value) + self.suit
 
"""This class creates the deck for the game by adding a unique card made in class Card()"""
class Deck(object):
    def __init__(self):
        self.deck = []
        values = ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"]
        suits = ["\u2663", "\u2666", "\u2660", "\u2665"]
        for suit in suits:
            for value in values:
                self.deck.append(Card(value, suit))
    
    def shuffle_deck(self): #Shuffle the deck
        random.shuffle(self.deck)

    def deal_cards(self): # Deal the cards
        return self.deck.pop(0)

    def __repr__(self):
        return str(self.deck)
    
"""This class will be the player's hand represented as a list"""
class Player(object):
  def __init__(self):
    self.hand = []

"""This class will be the computer's hand represented as a list"""    
class Computer(object):
  def __init__(self):
    self.hand = []  

"""This class will get the scores of a hand
    The point system we chose is highest card: value of higest card, pairs: 50 + pair card value, flush: 75pts, straight: 100 + highestCard, triple = 125 + card_value"""                                 
class Scores(object):

    def __init__(self, hand):
        self.hand = hand
        
    def __repr__(self):
        return str(self.deck)
    
    def int_values_list(self, value): # Creates a list with the values of each card from the hand into  list
        values = [] # This list will include just the values of the hand 
        for hand in self.hand:
            if hand.value == 'J':
                values.append('11')
            elif hand.value == 'Q':
                 values.append('12')
            elif hand.value == 'K':
                 values.append('13')
            elif hand.value == 'A':
                 values.append('14')
            else:
                values.append(hand.value)
        
        for i in range(0, len(values)): 
            values[i] = int(values[i]) # Convert string values to integers
        return values
        
    def highest_card(self, value): # Gets the highest card in the hand
        values = self.int_values_list(value)
        return max(values)
    
    def flush(self, suit): #All Cards are the same suit
        max_points = 75
        suits = [] # first suit stored in the list
        #For loop for only suits in hand
        for hand in self.hand:
           suits.append(hand.suit)
        if suits[0] == suits[1] == suits[2]:
            return max_points
        return 0 
    
    def straight(self, value): # Checks for straight, puts the straight in a list and returns the list
       #Values below is checking if it is flush 
        max_points = 100
        values = self.int_values_list(value) #Calls for values from deck only
        values.sort() #Sort the hand from shortest to highest element
        check1 = values[1] - values[0]
        check2 = values[2] - values[1]
        
        if check1 == check2 == 1:
            last_card = values[2]
            return max_points + last_card     
        return 0

    def get_values(self, values): # Returns the number of times an element appears on the list
        times = 0
        card_value = None
        no_pair = 0
        pair = 50
        triple = 125
        values = [] # This list will include just the values 
        
        for hand in self.hand:
            values.append(hand.value)      
        int_values = self.int_values_list(values) #Get the values in a integer list.
            
        for value in int_values:
            if int_values.count(value) > times:
                times = int_values.count(value)
                card_value = value # Gets the value of the pair number
        if times == 2:
            return pair + card_value
        elif times == 3:
            return triple + card_value
        return no_pair #Added card_value to compare in case of both have pairs or three pairs incase one is bigger

"""This class checks the hand of a player/computer"""    
class CheckHand(Scores):
    def __init__(self, hand):
        self.hand = hand
        
    def __repr__(self):
        return str(self.hand)
    
    def check_hand(self, value):
        highest_card = super().highest_card(self.hand) # The super() function is a special function that allows us to call a method form the parent class
        values = super().get_values(self.hand)
        flush = super().flush(self.hand)
        straight = super().straight(self.hand)
        straight_flush = straight + flush

        # Uses hierarchy of the best hand to the worst and return each values
        if straight_flush >= 177 and straight_flush <= 189:
            return "a Straight Flush", straight_flush
        elif values >= 127 and values <= 139:
            return "a Three of a Kind", values
        elif straight >= 102 and straight <= 114:
            return "a Straight", straight
        elif flush == 75:
            return "a Flush", flush
        elif values >= 52 and values <= 64:
            return "a Pair", values
        else:
            return "a High Card", highest_card
    
"""This function will display the entire poker game from the user interface """                
class Game(object):
    
    def __init__(self):
        self.hand = []
        
    def user_raise(self,bet, wallet): # Checks if the user wants to raise the old bet
            re_bet = int(input("\nWould you like to raise or do no addtional bet? press '1' for raise, press '2' for no additional actions: "))
            if re_bet == 1:
                new_bet = bet + int(input("\nHow much do you want to raise?: "))
                if new_bet > wallet:
                    print("You don't have enough in your wallet. You cannot raise, old bet will stand ")
                    new_bet = bet
            else:
                 new_bet = bet
            return new_bet

    # Checks the hands of both the computer and user and decides which one has the highest score, returning the winner and updating the wallets
    # Gameplay without AI to make it fairly with the user
    def checkWinner(self, player_hand, computer_hand, value_computer, value_user, new_bet, user_wallet, computer_wallet):
        print("Wallet: ", user_wallet)
        print("\nComputer's turn")
        print("\nComputer matches the new bet. ")
        print("\nPlayer gets: ", player_hand[0], " | ", player_hand[1]," | ", player_hand[2], "\n")
        print("Computer gets:", computer_hand[0]," | ", computer_hand[1]," | ", computer_hand[2], "\n") 
        
        if value_computer[1] > value_user[1]: # Checks if computer is the winner
            print("\nComputer wins because it got: ", value_computer[0])
            computer_wallet = computer_wallet + new_bet
            user_wallet = user_wallet - new_bet # Adds users bet to computer's wallet
        elif value_computer[1] < value_user[1]: # Checks if user gets money
            print("\nPlayer wins because it got:", value_user[0]) 
            user_wallet = new_bet + user_wallet # Adds to user wallet
            computer_wallet = computer_wallet - new_bet # Substracts from computer wallet 
        else:
            print("\nIt is a tie, money back")       
        return computer_wallet, user_wallet
    
    # AI function taking advantage of informationfrom the values before any computer decision is made
    # Compares hands beforehand and decides whether the computer should fold, raise, or match
    # This will make the computer either win or just fold and lose the bet, so it will lose less money if it has a bad hand
    # and will have a greater chance to win money by calculating when it is going to win
    def computer_brain(self, player_hand, computer_hand, value_computer, value_user, new_bet, user_wallet, computer_wallet, bet):
        print("\nit is Computer's turn: ")
        comp_bet = 0
        if value_computer[1] < value_user[1]: # Computer score worse than user score, then fold
            computer_wallet = computer_wallet - bet #Computer always loses just the initial bet
            user_wallet = user_wallet + new_bet
            print("\nComputer folds, user wins")
            
        elif value_computer[1] > value_user[1]: # Computer score greater than user score
            if new_bet >= int(user_wallet * 0.5): # Then computer matches the user bet if the bet is greater than 50% of the wallet
                print("\nComputer matches the bet by ", new_bet)
                print("\nPlayer gets: ",  " | ", player_hand[0], " | ", player_hand[1], " | ", player_hand[2], " | ")
                print("\nComputer gets: ", " | ", computer_hand[0], " | ", computer_hand[1], " | ", computer_hand[2], " | ")
                computer_wallet = computer_wallet + new_bet
                user_wallet = user_wallet - new_bet
                print("\nComputer wins because it got: ", value_computer[0])
            elif new_bet <= int(user_wallet * 0.5): # Or computer raises the user bet if the bet is less than 40% of the wallet
                comp_bet = new_bet + int(new_bet * 0.4) # Computer bet rased 40% of user's bet
                print("\nComputer raises bet to ", comp_bet)
                answer = int(input("Do you want to match or fold? press '1' to fold, and '2'match:"))
                if answer == 1: #Player folds after the first bet
                    computer_wallet = computer_wallet + new_bet
                    user_wallet = user_wallet - new_bet
                    print("\nPlayer folds")
                elif answer == 2: # Player matches computer's bet
                    match_bet = new_bet + (comp_bet - new_bet) #Created this match variable up with comp_bet and bet
                    print("Player matches by ", match_bet)
                    print("\nPlayer gets: ",  " | ", player_hand[0], " | ", player_hand[1], " | ", player_hand[2])
                    print("\nComputer gets: ", " | ", computer_hand[0], " | ", computer_hand[1], " | ", computer_hand[2])
                    computer_wallet = computer_wallet + match_bet
                    user_wallet = user_wallet - match_bet
                    print("Computer wins because it got: ", value_computer[0])
        else: # It is a tie
            print("\nComputer matches the bet")
            print("\nPlayer gets: ",  " | ", player_hand[0], " | ", player_hand[1], " | ", player_hand[2])
            print("\nComputer gets: ", " | ", computer_hand[0], " | ", computer_hand[1], " | ", computer_hand[2])
            print("\nIt is a tie, money back")
        return computer_wallet, user_wallet
    

    # Gameplay function that uses a while loop until the user wants to quit the game
    # Generates the deck, shuffles, deal cards
    # Count and computer_wallet variables is a part of AI by calling the computer_brain function if count = 0 or computer_hand is negative   
    def poker(self):
        print("""Welcome to the 3 cards Poker Game.
            Basic rules:
            1) You will place an intial bet after adding money into your wallet
            2) 3 cards will be dealt, one of them will be faced down.
            2) Once the cards are dealt, you would be allowed to either fold, raise, or match a bet while playing against the computer.
            4) Highest hand will win.
            5) An Ace is always counted as the highest card. It will never be counted as its lowest value
            6) If two hands tie by highest card, then money will be split into the two parties.
            7) If two hands tie by a Flush, then money will be split between the two parties.""")
        play = True
        user_wallet = int(input("\nHow much do you want to add to your wallet? Initial quantities must be between $100 and $1,000: "))
        while user_wallet < 100 or user_wallet > 1000: # Checks the money added to the wallet is between 100 and 1,000
            user_wallet = int(input("How much do you want to add to your wallet? Initial quantities must be between $100 and $1,000: "))
            
        computer_wallet = 0 #Keeps track if the computer is getting any profit or loosing money
        count = random.randint(0,4) #Gets a random number so the algorithm will be perform when this value is 0
        while(play):
            
            deck = Deck() # Creating the deck
            player = Player() # Creating the users hand
            computer = Computer() # Creating the computers hand
            score_user = CheckHand(player.hand)  #Checks score of user hand
            score_computer = CheckHand(computer.hand) # Checks the score of the computer hand 
            bet = 0 # Resets every round
            deck.shuffle_deck() #Shuffles deck
            if user_wallet == 0: # Checks if the user has enough money
                print("Not enough money in wallet, get add to wallet. ")
                print("Wallet: ", user_wallet)
                user_wallet = int(input("How much do you want to add to your wallet? Between $100 and $1,000: "))
            print("\nWallet: ", user_wallet)
            print("\nWallet: ", computer_wallet)
            print("\nDeck of card gets shuffled")
            bet = int(input("\nHow much do you want to bet: "))
            
            while bet > user_wallet:     
                bet = int(input("\nHow much do you want to bet: "))
                
            # for loop calls for a new card from deck and appends to user hand and computer hand
            for i in range(3):
                card_user = deck.deal_cards() #deals out cards to user
                player.hand.append(card_user)
                card_comp = deck.deal_cards() #deals out cards to user
                computer.hand.append(card_comp)
            
            player_hand = player.hand
            computer_hand = computer.hand
            value_computer = score_computer.check_hand(computer.hand) #Gets the score of computer hand
            value_user = score_user.check_hand(player.hand) #Gets the score of player hand 
             

            print("\nPlayer gets: ",  " | ", player_hand[0], " | ", player_hand[1], " | ", "XX" )
            print("\nComputer gets: ", " | ", computer_hand[0], " | ", computer_hand[1], " | ", "XX")
            fold = int(input("\nDo you want to fold or do you want to keep on playing? press '1' to fold, and '2' to keep on playing: "))
                
            if(fold > 1):
                new_bet = self.user_raise(bet, user_wallet) 
                 
                if count == 0 or computer_wallet < 0: # Checks if computer_wallet is negative or if the random counter is at 0 to call the AI algorithm
                    if value_computer[1] < value_user[1]:
                        player_hand.pop(2)
                        card_user = deck.deal_cards()
                        player_hand.insert(2, card_user)
                        computer_hand.pop(2)
                        card_comp = deck.deal_cards()
                        computer_hand.insert(2, card_comp)
                        value_computer = score_computer.check_hand(computer.hand) #Gets the score of computer hand
                        value_user = score_user.check_hand(player.hand) #Gets the score of player hand
                        new_wallets = self.computer_brain(player_hand, computer_hand, value_computer, value_user, new_bet, user_wallet, computer_wallet, bet)
                        user_wallet = new_wallets[1]
                        computer_wallet = new_wallets[0]
                        play_again = int(input("\nDo you want to play again? press '1' for yes press '2' for no: "))
                        if play_again == 2:
                            play = False
                    else:
                        new_wallets = self.computer_brain(player_hand, computer_hand, value_computer, value_user, new_bet, user_wallet, computer_wallet, bet)
                        user_wallet = new_wallets[1]
                        computer_wallet = new_wallets[0]
                        play_again = int(input("\nDo you want to play again? press '1' for yes press '2' for no: "))
                        if play_again == 2:
                            play = False
                    
                elif new_bet >= int(user_wallet * 0.5) and value_computer[1] < value_user[1]:
                    computer_wallet = computer_wallet - bet #Computer always loses just the initial bet
                    user_wallet = user_wallet + new_bet
                    print("\nComputer's turn")
                    print("\nComputer folds, user wins") # Computer folds if the bet is 4 times the amount the user has already bet
                    play_again = int(input("\nDo you want to play again? press '1' for yes press '2' for no: "))
                    if play_again == 2:
                        play = False

                else:
                    new_wallets = self.checkWinner(player_hand, computer_hand, value_computer, value_user, new_bet, user_wallet, computer_wallet)                        
                    user_wallet = new_wallets[1]
                    computer_wallet = new_wallets[0]
                    play_again = int(input("\nDo you want to play again? press '1' for yes press '2' for no: "))
                    if play_again == 2:
                        play = False
            else:
                user_wallet = user_wallet - bet
                computer_wallet = computer_wallet + bet
                play_again = int(input("\nDo you want to play again? press '1' for yes press '2' for no: "))
                if play_again == 2:
                    play = False
        return computer_wallet

if __name__ == "__main__": # Executes the code
    c = Game()
    c.poker()

